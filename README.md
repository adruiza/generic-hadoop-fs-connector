# Generic connector for Hadoop

This Maven project compiles a *generic* file system connector for Hadoop. There is some information you need to know before you start creating your own connector for your file system. This is a WIP, so don't expect everything to work out of the box. In the future, the process will be much more automated.


## Namespace changes

The same Hadoop installation needs to be compatible with multiple connectors for a range of file systems. For this to work, the generic connector **must** be specialized for the file system to be integrated. This means changes in the source code, but hopefully this will be less tedious than programming your own connector.

All Java class names, packages and folder structure have been recreated with the *generic* file system in mind. This name must be changed in the following locations with your file system identifier (**MySystem** for this instructions):

### Common tasks

- [Optional] Main project folder, **Generic Connector**, shall be renamed to **MySystem Connector**. This is a cosmetic change and has no effect in the compilation process.

### Related to the Java submodule

- Java submodule directory tree, src/main/java/org/apache/hadoop/fs/connector/**generic**, shall be renamed to src/main/java/org/apache/hadoop/fs/connector/**mysystem**.
- Java files shall be renamed from **Generic\*.java** to **MySystem\*.java**.
- The package root for all .java files shall be changed from org.apache.hadoop.fs.connector.**generic** to org.apache.hadoop.fs.connector.**mysystem**.
- For all class names, the **Generic** keyword must be changed to **MySystem**. The imports in **GenericFileSystem** and **GenericAbstractFileSystem** shall be modified to reflect this changes to that classes.
- The line with System.loadLibrary("**generic**") must be changed to System.loadLibrary("**mysystem**").

### Related to the C submodule:

- In jni_connector.c, it is safe to rename every **generic** reference to **mysystem** preserving case. This means "GENERIC" turns to "MYSYSTEM", "Generic" turns to "MySystem" and "generic" turns to "mysystem".

### Related to Maven pom.xml files:

- The pom.xml located at "native/linux" needs a new artifactId. From lib**generic** to lib**mysystem** is the way for it to be found later by the JVM through JNI.

In the future, this specialization of the source code may be done automatically through a script, but for the time being this is the only road available.


## File system implementation

The file system to be integrated into Hadoop needs to fill every *filesystem.c* function in order to satisfy its header. Currently, they only return 0 as they do not implement anything.

If you need to link to your own libraries or point to your custom headers, the C linker and compiler options in the pom.xml file in "native/linux" can be changed any way you want to satisfy your needs. Make sure that the Hadoop environment script reflects any custom paths defined there, or else the libraries may not be correctly located later.

Parallel support is yet to be improved, specially when handling multiple files at the same time.


## Output files


The result of the compilation process consists of two libraries. The first one is a .jar and the second one is a .so.

They are built inside "target" folders in their respective Maven submodules. They should be move to two locations for them to be recognized by Hadoop.

The .jar library needs to be moved to **share/hadoop/common/lib** and the .so library needs to be moved to **lib/native**. This paths are relative to the Hadoop installation.

Moreover, the core-site.xml file located in "etc/hadoop" should reflect the new implementation. This is achived by adding **fs.mysystem.impl** and **fs.AbstractFileSystem.mysystem.impl** as properties, and pointing to the respective Java class.