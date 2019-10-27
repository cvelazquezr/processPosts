package word2vec

import java.io.{File, IOException}
import java.net.URL
import java.util.zip.ZipFile

import org.apache.bcel.classfile.{ClassParser, JavaClass, Method}
import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._
import utils.Utils.splitCamelCase

class LibraryVector(groupID: String, artifactID: String, version: String) {
  val mavenAddress: String = "http://repo1.maven.org/maven2/"
  val path: String = "jars/" + pathJar()

  def downloadJar(): Unit = {
    if (!new File(path).exists())
      download(path, mavenAddress + pathJar())
  }

  def pathJar(): String = {
    groupID.replaceAll("\\.", "/") + "/" +
      artifactID.replaceAll("\\.", "/") + "/" +
      version + "/" +
      artifactID + "-" + version + ".jar"
  }

  def download(target: String, url: String): Option[ZipFile] = {
    val file: File = new File(target)
    try {
      if (!file.exists())
        FileUtils.copyURLToFile(new URL(url), new File(target))

      val zipFile: ZipFile = new ZipFile(target)
      Some(zipFile)
    } catch {
      case _: IOException => None
    }
  }

  def getModules(): String = {
    downloadJar()
    val file: File = new File(path)

    if (file.exists()) {
      val zipFile = new ZipFile(file)
      val textToWrite = new StringBuilder

      val entries = zipFile.entries.asScala
      entries.filter(x => !x.isDirectory && x.getName.endsWith(".class")).foreach(entry => {
        val entryName: String = entry.getName

        if (!entryName.equals("module-info.class")) {
          val classParser: ClassParser = new ClassParser(zipFile.getInputStream(entry), entry.getName)
          val javaClass: JavaClass = classParser.parse()

          if (javaClass.isPublic || !(javaClass.isPrivate & javaClass.isProtected)) {
            var methodsNames: Array[String] = Array()

            val classNamePath: Array[String] = javaClass.getClassName.split("\\.")
            val className: String = classNamePath.takeRight(1)(0)

            textToWrite.append(splitCamelCase(className)).append(" ")
            val methods: Array[Method] = javaClass.getMethods

            methods.filter(method => method.isPublic || !(method.isPrivate && method.isProtected)).foreach(method => {
              val methodName: String = method.getName
              if (!methodName.startsWith("<") && !methodsNames.contains(methodName) && !methodName.contains("$")) {
                methodsNames :+= methodName
                textToWrite.append(splitCamelCase(methodName)).append(" ")
              }
            })
          }
        }
      })
      zipFile.close()

      textToWrite.toString().trim
    }
    else
      ""
  }

  def getOnlyClasses(): String = {
    downloadJar()
    val file: File = new File(path)

    if (file.exists()) {
      val zipFile = new ZipFile(file)
      val textToWrite = new StringBuilder

      val entries = zipFile.entries.asScala
      entries.filter(x => !x.isDirectory && x.getName.endsWith(".class")).foreach(entry => {
        val entryName: String = entry.getName

        if (!entryName.equals("module-info.class")) {
          val classParser: ClassParser = new ClassParser(zipFile.getInputStream(entry), entry.getName)
          val javaClass: JavaClass = classParser.parse()

          if (javaClass.isPublic || !(javaClass.isPrivate & javaClass.isProtected)) {

            val classNamePath: Array[String] = javaClass.getClassName.split("\\.")
            var className: String = classNamePath.takeRight(1)(0)

            if (className.contains("$"))
              className = className.split("\\$").mkString(" ")

            textToWrite.append(className).append(" ")
          }
        }
      })
      zipFile.close()

      textToWrite.toString().trim
    }
    else
      ""
  }

  def getClassesMethods(): String = {
    downloadJar()
    val file: File = new File(path)

    if (file.exists()) {
      val zipFile = new ZipFile(file)
      val textToWrite = new StringBuilder

      val entries = zipFile.entries.asScala
      entries.filter(x => !x.isDirectory && x.getName.endsWith(".class")).foreach(entry => {
        val entryName: String = entry.getName

        if (!entryName.equals("module-info.class")) {
          val classParser: ClassParser = new ClassParser(zipFile.getInputStream(entry), entry.getName)
          val javaClass: JavaClass = classParser.parse()

          if (javaClass.isPublic || !(javaClass.isPrivate & javaClass.isProtected)) {

            val classNamePath: Array[String] = javaClass.getClassName.split("\\.")
            var className: String = classNamePath.takeRight(1)(0)

            if (className.contains("$"))
              className = className.split("\\$").mkString(" ")

            textToWrite.append(className).append(" ")

            val methods: Array[Method] = javaClass.getMethods
            methods.foreach(method => {
              if (method.isPublic || !(method.isPrivate & method.isProtected)) {
                textToWrite.append(method.getName).append(" ")
              }
            })
          }
        }
      })
      zipFile.close()

      textToWrite.toString().trim
    }
    else
      ""
  }

  def getModulesToCode(): String = {
    downloadJar()
    val file: File = new File(path)

    if (file.exists()) {
      val zipFile = new ZipFile(file)
        val textToWrite = new StringBuilder

        val entries = zipFile.entries.asScala
        entries.filter(x => x.isDirectory).foreach(entry => {
          val entryName: String = entry.getName
          if (!entryName.startsWith("META-INF")) {
            val replacement: String = entryName.replaceAll("/", "\\.")
            textToWrite.append("import ").append(replacement.dropRight(1)).append(".*;\n")
          }
        })
        zipFile.close()

        textToWrite.toString().trim
    } else ""
  }

  def getModulesToCodePattern(): String = {
    downloadJar()
    val file: File = new File(path)

    if (file.exists()) {
      val zipFile = new ZipFile(file)
      val textToWrite = new StringBuilder

      val entries = zipFile.entries.asScala
      entries.filter(x => x.isDirectory).foreach(entry => {
        val entryName: String = entry.getName
        if (!entryName.startsWith("META-INF")) {
          val replacement: String = entryName.replaceAll("/", "\\.")
          textToWrite.append(replacement.dropRight(1)).append(" ")
        }
      })
      zipFile.close()

      textToWrite.toString().trim
    }
    else
      ""
  }

  def getMethods(clazz: String): String = {
    downloadJar()
    val file: File = new File(path)

    if (file.exists()) {
      val zipFile = new ZipFile(file)
      val textToWrite = new StringBuilder

      val entries = zipFile.entries.asScala
      entries.filter(x => !x.isDirectory && x.getName.endsWith(".class")).foreach(entry => {
        val entryName: String = entry.getName

        if (!entryName.equals("module-info.class")) {
          val classParser: ClassParser = new ClassParser(zipFile.getInputStream(entry), entry.getName)
          val javaClass: JavaClass = classParser.parse()

          if (javaClass.isPublic || !(javaClass.isPrivate & javaClass.isProtected)) {

            val classNamePath: Array[String] = javaClass.getClassName.split("\\.")
            var className: String = classNamePath.takeRight(1)(0)

            if (className.contains("$"))
              className = className.split("\\$").mkString(" ")

            if (className.equals(clazz)) {
              val methods: Array[Method] = javaClass.getMethods
              methods.foreach(method => {
                if (method.isPublic || !(method.isPrivate & method.isProtected)) {
                  textToWrite.append(method.getName).append(" ")
                }
              })
            }
          }
        }
      })
      zipFile.close()

      textToWrite.toString().trim
    }
    else
      ""
  }

}
