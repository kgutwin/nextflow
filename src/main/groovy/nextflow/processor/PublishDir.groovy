/*
 * Copyright (c) 2013-2016, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2016, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.processor
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.PackageScope
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
/**
 * Implements the {@code publishDir} directory. It create links or copies the output
 * files of a given task to a user specified directory.
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@ToString
@EqualsAndHashCode
class PublishDir {

    enum Mode { SYMLINK, LINK, COPY, MOVE }

    /**
     * The target path where create the links or copy the output files
     */
    Path path

    /**
     * Whenever overwrite existing files
     */
    Boolean overwrite

    /**
     * The publish {@link Mode}
     */
    Mode mode

    /**
     * A glob file pattern to filter the files to be published
     */
    String pattern

    private PathMatcher matcher

    private FileSystem sourceFileSystem

    private TaskProcessor processor

    private static ExecutorService executor

    void setPath( Closure obj ) {
        setPath( obj.call() as Path )
    }

    void setPath( String str ) {
        setPath(str as Path)
    }

    void setPath( Path obj ) {
        this.path = obj.complete()
    }

    void setMode( String str ) {
        this.mode = str.toUpperCase() as Mode
    }

    void setMode( Mode mode )  {
        this.mode = mode
    }

    /**
     * Object factory method
     *
     * @param obj When the {@code obj} is a {@link Path} or a {@link String} object it is
     * interpreted as the target path. Otherwise a {@link Map} object matching the class properties
     * can be specified.
     *
     * @return An instance of {@link PublishDir} class
     */
    static PublishDir create( obj ) {
        def result
        if( obj instanceof Path ) {
            result = new PublishDir(path: obj)
        }
        else if( obj instanceof List ) {
            result = createWithList(obj)
        }
        else if( obj instanceof Map ) {
            result = createWithMap(obj)
        }
        else if( obj instanceof CharSequence ) {
            result = new PublishDir(path: obj)
        }
        else {
            throw new IllegalArgumentException("Not a valid `publishDir` directive: ${obj}" )
        }

        if( !result.path ) {
            throw new IllegalArgumentException("Missing path in `publishDir` directive")
        }

        return result
    }

    static private PublishDir createWithList(List entry) {
        assert entry.size()==2
        assert entry[0] instanceof Map

        def map = new HashMap((Map)entry[0])
        map.path = entry[1]

        createWithMap(map)
    }

    static private PublishDir createWithMap(Map map) {
        assert map

        def result = new PublishDir()
        if( map.path ) {
            result.path = map.path
        }

        if( map.mode )
            result.mode = map.mode

        if( map.pattern )
            result.pattern = map.pattern

        if( map.overwrite != null )
            result.overwrite = map.overwrite

        return result
    }

    /**
     * Apply the publishing process to the specified {@link TaskRun} instance
     *
     * @param task The task whose output need to be published
     */
    @CompileStatic
    void apply( List<Path> files, TaskProcessor processor = null ) {

        if( !files ) {
            return
        }

        this.processor = processor
        this.sourceFileSystem = files[0].fileSystem

        createPublishDir()

        validatePublishMode()

        /*
         * when the publishing is using links, create them in process
         * otherwise copy and moving file can take a lot of time, thus
         * apply the operation using an external thread
         */
        final inProcess = mode == Mode.LINK || mode == Mode.SYMLINK

        if( pattern ) {
            this.matcher = FileHelper.getPathMatcherFor("glob:${pattern}", sourceFileSystem)
        }

        if( !inProcess ) {
            createExecutor()
        }

        /*
         * iterate over the file parameter and publish each single file
         */
        files.each { value ->
            apply(value, inProcess)
        }
    }


    @CompileStatic
    protected void apply( Path source, boolean inProcess ) {

        final name = source.getFileName()
        if( matcher && !matcher.matches(name) ) {
            // skip not matching file
            return
        }

        final destination = path.resolve(name.toString())
        if( inProcess ) {
            processFile(source, destination)
        }
        else {
            executor.submit({ processFile(source, destination) } as Runnable)
        }

    }

    @CompileStatic
    protected void processFile( Path source, Path destination ) {

        try {
            processFileImpl(source, destination)
        }
        catch( FileAlreadyExistsException e ) {
            if( overwrite ) {
                FilesEx.deleteDir(destination)
                processFileImpl(source, destination)
            }
        }
    }

    @CompileStatic
    protected void processFileImpl( Path source, Path destination ) {
        if( !mode || mode == Mode.SYMLINK ) {
            Files.createSymbolicLink(destination, source)
        }
        else if( mode == Mode.LINK ) {
            FilesEx.mklink(source, [hard:true], destination)
        }
        else if( mode == Mode.MOVE ) {
            FilesEx.moveTo(source, destination)
        }
        else if( mode == Mode.COPY ) {
            FilesEx.copyTo(source, destination)
        }
        else {
            throw new IllegalArgumentException("Unknown file publish mode: ${mode}")
        }
    }

    @CompileStatic
    private void createPublishDir() {
        try {
            Files.createDirectories(this.path)
        }
        catch( FileAlreadyExistsException e ) {
            // ignore
        }
    }

    /*
     * That valid publish mode has been selected
     * Note: link and symlinks are not allowed across different file system
     */
    @CompileStatic
    @PackageScope
    void validatePublishMode() {

        if( sourceFileSystem != path.fileSystem ) {
            if( !mode ) {
                mode = Mode.COPY
            }
            else if( mode == Mode.SYMLINK || mode == Mode.LINK ) {
                mode = Mode.COPY
                if( processor )
                    processor.warn("Cannot use mode `${mode.toString().toLowerCase()}` to publish files to path: $path -- Using mode `copy` instead")
            }
        }

        if( !mode ) {
            mode = Mode.SYMLINK
        }
    }

    @CompileStatic
    static synchronized private void createExecutor() {
        if( !executor ) {
            executor = new ThreadPoolExecutor(0, Runtime.runtime.availableProcessors(),
                    60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());

            // register the shutdown on termination
            def session = Global.session as Session
            if( session ) {
                session.onShutdown {
                    executor.shutdown()
                    executor.awaitTermination(36,TimeUnit.HOURS)
                }
            }
        }
    }

    @PackageScope
    static ExecutorService getExecutor() { executor }
}