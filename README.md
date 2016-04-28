# MapReduce Fuzzy Join Algorithms (Edit and Jaccard Distance)


## ABOUT

This is the set of fuzzy join algorithms tested in an upcoming paper.

The algorithms each execute a fuzzy join using Edit Distance and Jaccard Similarity. Edit Distance uses strings of text; Jaccard Similarity uses sets of integers. Output is textual to be human-readable.

### List of all files/folders:

+ FuzzyJoin\*Edit.java, FuzzyJoin*Jaccard.java - source for the different Fuzzy Join workflows.

+ utilities/HadoopPreprocessIntArrays.java - source for a tool that takes setID-setItem pairs and creates a .seq (input) file with sets grouped by setID.

+ datatransfer/*.java - source for custom Writable (data-transfer) formats needed by various of the algorithms.

+ subroutines/*.java - source for Edit Distance comparators and Aho-Korasick keyword tree structure used by the algorithms.

+ jar/*.jar - compiled versions of all of the above source files.

+ ca/uvic/csc/research/*.class - The compiled components that went into the above JAR files. The somewhat awkward folder structure is because of the package name used.

+ LICENSE - This code is using the MIT license, so you can do absolutely anything with it as long as this file comes along.

+ README.md - This file.


## COMPILING
*Please note that the default GitHub package includes precompiled jar files in the jar/ folder. You shouldn't need this section most of the time.*

To compile these programs, you’ll need an installation of Hadoop. The algorithms were made for Hadoop 1.2.1, but other 1.x versions will probably run them.

First, cd to the directory that houses this file:

### Compiling the data transfer classes [needed for the next bits]:

javac -classpath path_to_your_hadoop_install/hadoop-core-1.2.1.jar -d ca/uvic/csc/research/ datatransfer/*Writable.java 

The version numbers and ‘path_to_your_hadoop_install’ may vary depending on the release and location of your Hadoop installation.


### Compiling the Hadoop jobs:

javac -classpath path_to_your_hadoop_install/hadoop-core-1.2.1.jar:path_to_your_hadoop_install/lib/commons-lang-2.4.jar:. -d . FuzzyJoin*.java


### Compiling the utilities:

javac -classpath path_to_your_hadoop_install/hadoop-core-1.2.1.jar:. -d . utilities/HadoopPreprocessIntArrays.java

