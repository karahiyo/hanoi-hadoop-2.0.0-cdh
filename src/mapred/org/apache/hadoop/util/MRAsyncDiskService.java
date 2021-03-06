/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CleanupQueue;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class is a container of multiple thread pools, each for a volume,
 * so that we can schedule async disk operations easily.
 * 
 * Examples of async disk operations are deletion of files.
 * We can move the files to a "toBeDeleted" folder before asychronously
 * deleting it, to make sure the caller can run it faster.
 * 
 * Users should not write files into the "toBeDeleted" folder, otherwise
 * the files can be gone any time we restart the MRAsyncDiskService.  
 * 
 * This class also contains all operations that will be performed by the
 * thread pools. 
 */
public class MRAsyncDiskService {
  
  public static final Log LOG = LogFactory.getLog(MRAsyncDiskService.class);
  boolean shouldBeSecure = UserGroupInformation.isSecurityEnabled();
  AsyncDiskService asyncDiskService;
  TaskController taskController;
  private CleanupQueue cleanupQueue;
  
  public static final String TOBEDELETED = "toBeDeleted";
  
  /**
   * Create a AsyncDiskServices with a set of volumes (specified by their
   * root directories).
   * 
   * The AsyncDiskServices uses one ThreadPool per volume to do the async
   * disk operations.
   * 
   * @param localFileSystem The localFileSystem used for deletions.
   * @param nonCanonicalVols The roots of the file system volumes, which may
   * be absolte paths, or paths relative to the ${user.dir} system property
   * ("cwd").
   */
  public MRAsyncDiskService(FileSystem localFileSystem,
      TaskController taskController,
      String[] nonCanonicalVols) throws IOException {
    this.taskController = taskController;
    this.cleanupQueue = CleanupQueue.getInstance();
    this.localFileSystem = localFileSystem;
    this.volumes = new String[nonCanonicalVols.length];
    for (int v = 0; v < nonCanonicalVols.length; v++) {
      this.volumes[v] = normalizePath(nonCanonicalVols[v]);
      LOG.debug("Normalized volume: " + nonCanonicalVols[v]
          + " -> " + this.volumes[v]);
    }  
    
    asyncDiskService = new AsyncDiskService(this.volumes);
    
    // Create one ThreadPool per volume
    for (int v = 0 ; v < volumes.length; v++) {
      // Create the root for file deletion
      Path absoluteSubdir = new Path(volumes[v], TOBEDELETED);
      if (!localFileSystem.mkdirs(absoluteSubdir)) {
        // We should tolerate missing volumes. 
        LOG.warn("Cannot create " + TOBEDELETED + " in " + volumes[v] + ". Ignored.");
      }
    }
    
    // Create tasks to delete the paths inside the volumes
    for (int v = 0 ; v < volumes.length; v++) {
      Path absoluteSubdir = new Path(volumes[v], TOBEDELETED);
      FileStatus[] files = null;
      try {
        // List all files inside the volumes TOBEDELETED sub directory
        files = localFileSystem.listStatus(absoluteSubdir);
      } catch (Exception e) {
        // Ignore exceptions in listStatus
        // We tolerate missing sub directories.
      }
      if (files != null) {
        for (int f = 0; f < files.length; f++) {
          // Get the relative file name to the root of the volume
          String absoluteFilename = files[f].getPath().toUri().getPath();
          String relative = TOBEDELETED + Path.SEPARATOR_CHAR
              + files[f].getPath().getName();
          if (shouldBeSecure) {
            deletePathsInSecureCluster(absoluteFilename, files[f]);
          } else {
            DeleteTask task =
                new DeleteTask(volumes[v], absoluteFilename, relative,
                    files[f].getOwner());
            execute(volumes[v], task);
          }
        }
      }
    }
  }

  public MRAsyncDiskService(FileSystem localFileSystem,
                            String[] nonCanonicalVols) throws IOException {
    this(localFileSystem, null, nonCanonicalVols);
  }

  /**
   * Initialize MRAsyncDiskService based on conf.
   * @param conf  local file system and local dirs will be read from conf 
   */
  public MRAsyncDiskService(JobConf conf) throws IOException {
    this(FileSystem.getLocal(conf), conf.getLocalDirs());
  }

  /**
   * Execute the task sometime in the future, using ThreadPools.
   */
  synchronized void execute(String root, Runnable task) {
    asyncDiskService.execute(root, task);
  }
  
  /**
   * Gracefully start the shut down of all ThreadPools.
   */
  public synchronized void shutdown() {
    asyncDiskService.shutdown();
  }

  /**
   * Shut down all ThreadPools immediately.
   */
  public synchronized List<Runnable> shutdownNow() {
    return asyncDiskService.shutdownNow();
  }
  
  /**
   * Wait for the termination of the thread pools.
   * 
   * @param milliseconds  The number of milliseconds to wait
   * @return   true if all thread pools are terminated within time limit
   * @throws InterruptedException 
   */
  public synchronized boolean awaitTermination(long milliseconds) 
      throws InterruptedException {
    boolean result = asyncDiskService.awaitTermination(milliseconds);
    if (result) {
      LOG.info("Deleting toBeDeleted directory.");
      for (int v = 0; v < volumes.length; v++) {
        Path p = new Path(volumes[v], TOBEDELETED);
        try {
          localFileSystem.delete(p, true);
        } catch (IOException e) {
          LOG.warn("Cannot cleanup " + p + " " + StringUtils.stringifyException(e));
        }
      }
    }
    return result;
  }  
  
  private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS");
  
  private FileSystem localFileSystem;
  
  private String[] volumes; 
                 
  private static AtomicLong uniqueId = new AtomicLong(0);
  
  /** A task for deleting a pathName from a volume.
   */
  class DeleteTask implements Runnable {

    /** The volume that the file is on*/
    String volume;
    /** The file name before the move */
    String originalPath;
    /** The file name after the move */
    String pathToBeDeleted;
    String owner;
    
    /**
     * Delete a file/directory (recursively if needed).
     * 
     * @param volume The volume that the file/dir is in.
     * @param originalPath The original name, relative to volume root.
     * @param pathToBeDeleted The name after the move, relative to volume root,
     *          containing TOBEDELETED.
     * @param owner The owner of the file
     */
    DeleteTask(String volume, String originalPath, String pathToBeDeleted,
        String owner) {
      this.volume = volume;
      this.originalPath = originalPath;
      this.pathToBeDeleted = pathToBeDeleted;
    }
    
    @Override
    public String toString() {
      // Called in AsyncDiskService.execute for displaying error messages.
      return "deletion of " + pathToBeDeleted + " on " + volume
          + " with original name " + originalPath;
    }

    @Override
    public void run() {
      boolean success = false;
      Exception e = null;
      try {
        Path absolutePathToBeDeleted = new Path(volume, pathToBeDeleted);
        if (taskController != null & owner != null) {
          taskController
              .deleteAsUser(owner, absolutePathToBeDeleted.toString());
        } else {
          success = localFileSystem.delete(absolutePathToBeDeleted, true);
        }
      } catch (Exception ex) {
        e = ex;
      }
      
      if (!success) {
        if (e != null) {
          LOG.warn("Failure in " + this + " with exception "
              + StringUtils.stringifyException(e));
        } else {
          LOG.warn("Failure in " + this);
        }
      } else {
        LOG.debug("Successfully did " + this.toString());
      }
    }
  };
  
  
  /**
   * Move the path name on one volume to a temporary location and then 
   * delete them.
   * 
   * This functions returns when the moves are done, but not necessarily all
   * deletions are done. This is usually good enough because applications 
   * won't see the path name under the old name anyway after the move. 
   * 
   * @param volume       The disk volume
   * @param pathName     The path name relative to volume root.
   * @throws IOException If the move failed 
   * @return   false     if the file is not found
   */
  public boolean moveAndDeleteRelativePath(String volume, String pathName)
      throws IOException {
    
    volume = normalizePath(volume);
    
    // Move the file right now, so that it can be deleted later
    String newPathName = 
        format.format(new Date()) + "_" + uniqueId.getAndIncrement();
    newPathName = TOBEDELETED + Path.SEPARATOR_CHAR + newPathName;
    
    Path source = new Path(volume, pathName);
    Path target = new Path(volume, newPathName);
    try {
      if (!localFileSystem.rename(source, target)) {
        // If the source does not exists, return false.
        // This is necessary because rename can return false if the source  
        // does not exists.
        if (!localFileSystem.exists(source)) {
          return false;
        }
        // Try to recreate the parent directory just in case it gets deleted.
        if (!localFileSystem.mkdirs(new Path(volume, TOBEDELETED))) {
          throw new IOException("Cannot create " + TOBEDELETED + " under "
              + volume);
        }
        // Try rename again. If it fails, return false.
        if (!localFileSystem.rename(source, target)) {
          throw new IOException("Cannot rename " + source + " to "
              + target);
        }
      }
    } catch (FileNotFoundException e) {
      // Return false in case that the file is not found.  
      return false;
    }

    FileStatus status = localFileSystem.getFileStatus(target);

    if (shouldBeSecure) {
      deletePathsInSecureCluster(newPathName, status);
    } else {
      DeleteTask task =
          new DeleteTask(volume, pathName, newPathName, status.getOwner());
      execute(volume, task);
    }
    return true;
  }

  private void deletePathsInSecureCluster(String newPathName, FileStatus status)
      throws FileNotFoundException, IOException {
    // In a secure tasktracker, the subdirectories belong
    // to different user
    PathDeletionContext item = null;

    // iterate and queue subdirectories for cleanup
    for (FileStatus subDirStatus : localFileSystem.listStatus(status.getPath())) {
      String owner = subDirStatus.getOwner();
      String path = subDirStatus.getPath().getName();
      if (path.equals(owner)) {
        // add it to the cleanup queue

        item =
            new TaskController.DeletionContext(taskController, false, owner,
                newPathName + Path.SEPARATOR_CHAR + path);
        cleanupQueue.addToQueue(item);
      }
    }
    // queue the parent directory for cleanup
    item =
        new TaskController.DeletionContext(taskController, false,
            status.getOwner(), newPathName);
    cleanupQueue.addToQueue(item);
  }

  /**
   * Move the path name on each volume to a temporary location and then 
   * delete them.
   * 
   * This functions returns when the moves are done, but not necessarily all
   * deletions are done. This is usually good enough because applications 
   * won't see the path name under the old name anyway after the move. 
   * 
   * @param pathName     The path name relative to each volume root
   * @throws IOException If any of the move failed 
   * @return   false     If any of the target pathName did not exist,
   *                     note that the operation is still done on all volumes.
   */
  public boolean moveAndDeleteFromEachVolume(String pathName) throws IOException {
    boolean result = true; 
    for (int i = 0; i < volumes.length; i++) {
      result = result && moveAndDeleteRelativePath(volumes[i], pathName);
    }
    return result;
  }

  /**
   * Move specified directories/files in each volume into TOBEDELETED, and then
   * delete them.
   * 
   * @param dirsToDelete
   * @throws IOException
   */
  public void cleanupDirsInAllVolumes(String[] dirsToDelete)
      throws IOException {
    for (int v = 0; v < volumes.length; v++) {
      for (String entryName : dirsToDelete) {
        if (!TOBEDELETED.equals(entryName)) {
          moveAndDeleteRelativePath(volumes[v], entryName);
        }
      }
    }
  }

  /**
   * Move all files/directories inside volume into TOBEDELETED, and then
   * delete them.  The TOBEDELETED directory itself is ignored.
   */
  public void cleanupAllVolumes() throws IOException {
    for (int v = 0; v < volumes.length; v++) {
      // List all files inside the volumes
      FileStatus[] files = null;
      try {
        files = localFileSystem.listStatus(new Path(volumes[v]),
            TaskLog.USERLOGS_PATH_FILTER);
      } catch (Exception e) {
        // Ignore exceptions in listStatus
        // We tolerate missing volumes.
      }
      if (files != null) {
        for (int f = 0; f < files.length; f++) {
          // Get the file name - the last component of the Path
          String entryName = files[f].getPath().getName();
          // Do not delete the current TOBEDELETED
          if (!TOBEDELETED.equals(entryName)) {
            moveAndDeleteRelativePath(volumes[v], entryName);
          }
        }
      }
    }
  }
  
  /**
   * Returns the normalized path of a path.
   */
  private String normalizePath(String path) {
    return (new Path(path)).makeQualified(this.localFileSystem)
        .toUri().getPath();
  }
  
  /**
   * Get the relative path name with respect to the root of the volume.
   * @param absolutePathName The absolute path name
   * @param volume Root of the volume.
   * @return null if the absolute path name is outside of the volume.
   */
  private String getRelativePathName(String absolutePathName,
      String volume) {
    
    absolutePathName = normalizePath(absolutePathName);
    // Get the file names
    if (!absolutePathName.startsWith(volume)) {
      return null;
    }
    // Get rid of the volume prefix
    String fileName = absolutePathName.substring(volume.length());
    if (fileName.charAt(0) == Path.SEPARATOR_CHAR) {
      fileName = fileName.substring(1);
    }
    return fileName;
  }
  
  /**
   * Move the path name to a temporary location and then delete it.
   * 
   * Note that if there is no volume that contains this path, the path
   * will stay as it is, and the function will return false.
   *  
   * This functions returns when the moves are done, but not necessarily all
   * deletions are done. This is usually good enough because applications 
   * won't see the path name under the old name anyway after the move. 
   * 
   * @param absolutePathName    The path name from root "/"
   * @throws IOException        If the move failed
   * @return   false if we are unable to move the path name
   */
  public boolean moveAndDeleteAbsolutePath(String absolutePathName)
      throws IOException {
    
    for (int v = 0; v < volumes.length; v++) {
      String relative = getRelativePathName(absolutePathName, volumes[v]);
      if (relative != null) {
        return moveAndDeleteRelativePath(volumes[v], relative);
      }
    }
    
    throw new IOException("Cannot delete " + absolutePathName
        + " because it's outside of all volumes.");
  }
  
}
