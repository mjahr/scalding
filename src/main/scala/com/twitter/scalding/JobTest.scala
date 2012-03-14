package com.twitter.scalding

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.annotation.tailrec

import cascading.tuple.Tuple

import org.apache.hadoop.mapred.JobConf

object JobTest {
  def apply(jobName : String) = new JobTest(jobName)
}

/**
 * This class is used to construct unit tests for scalding jobs.
 * You should not use it unless you are writing tests.
 * For examples of how to do that, see the tests included in the
 * main scalding repository:
 * https://github.com/twitter/scalding/tree/master/src/test/scala/com/twitter/scalding
 */
class JobTest(jobName : String) extends TupleConversions {
  private var argsMap = Map[String, List[String]]()
  private val callbacks = Buffer[() => Unit]()
  private var sourceMap = Map[Source, Buffer[Tuple]]()
  private var sinkSet = Set[Source]()
  private var fileSet = Set[String]()

  def arg(inArg : String, value : List[String]) = {
    argsMap += inArg -> value
    this
  }

  def arg(inArg : String, value : String) = {
    argsMap += inArg -> List(value)
    this
  }

  def source(s : Source, iTuple : Iterable[Product]) = {
    sourceMap += s -> iTuple.toList.map{ productToTuple(_) }.toBuffer
    this
  }

  def sink[A](s : Source)(op : Buffer[A] => Unit )
    (implicit conv : TupleConverter[A]) = {
    val buffer = new ListBuffer[Tuple]
    sourceMap += s -> buffer
    sinkSet += s
    callbacks += (() => op(buffer.map{conv(_)}))
    this
  }

  // Simulates the existance of a file so that mode.fileExists returns true.  We
  // do not simulate the file contents; that should be done through mock
  // sources.
  def registerFile(filename : String) = {
    fileSet += filename
    this
  }

  // Registers test files and initializes the global mode.
  private def setMode(testMode : TestMode) = {
    testMode.registerTestFiles(fileSet)
    Mode.mode = testMode
  }

  def run = {
    setMode(Test(sourceMap))
    runAll(Job(jobName, new Args(argsMap)))
    this
  }

  def runHadoop = {
    setMode(HadoopTest(new JobConf(), sourceMap))
    runAll(Job(jobName, new Args(argsMap)))
    this
  }

  // This SITS is unfortunately needed to get around Specs
  def finish : Unit = { () }

  @tailrec
  final def runAll(job : Job) : Unit = {
    job.buildFlow.complete
    job.next match {
      case Some(nextjob) => runAll(nextjob)
      case None => {
        Mode.mode match {
          case Hdfs(_,_) | HadoopTest(_,_) =>
            sinkSet.foreach{ _.finalizeHadoopTestOutput(Mode.mode) }
          case _ => ()
        }
        // Now it is time to check the test conditions:
        callbacks.foreach { cb => cb() }
      }
    }
  }

  def runWithoutNext = {
    setMode(Test(sourceMap))
    Job(jobName, new Args(argsMap)).buildFlow.complete
    callbacks.foreach { cb => cb() }
    this
  }

}
