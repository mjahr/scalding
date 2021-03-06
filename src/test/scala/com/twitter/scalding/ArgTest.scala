package com.twitter.scalding
import org.specs._

class ArgTest extends Specification {
  "Tool.parseArgs" should {
    "handle the empty list" in {
      val map = Args(Array[String]())
      map.list("") must be_==(List())
    }
    "accept any number of dashed args" in {
      val map = Args(Array("--one", "1", "--two", "2", "--three", "3"))
      map.list("") must be_==(List())
      map.optional("") must be_==(None)

      map.list("absent") must be_==(List())
      map.optional("absent") must be_==(None)

      map("one") must be_==("1")
      map.list("one") must be_==(List("1"))
      map.required("one") must be_==("1")
      map.optional("one") must be_==(Some("1"))

      map("two") must be_==("2")
      map.list("two") must be_==(List("2"))
      map.required("two") must be_==("2")
      map.optional("two") must be_==(Some("2"))

      map("three") must be_==("3")
      map.list("three") must be_==(List("3"))
      map.required("three") must be_==("3")
      map.optional("three") must be_==(Some("3"))
    }
    "put initial args into the empty key" in {
      val map =Args(List("hello", "--one", "1"))
      map("") must be_==("hello")
      map.list("") must be_==(List("hello"))
      map.required("") must be_==("hello")
      map.optional("") must be_==(Some("hello"))

      map("one") must be_==("1")
      map.list("one") must be_==(List("1"))
    }
    "allow any number of args per key" in {
      val map = Args(Array("--one", "1", "--two", "2", "deux", "--zero"))
      map("one") must be_==("1")
      map.list("two") must be_==(List("2","deux"))
      map.boolean("zero") must be_==(true)
    }
    "allow any number of dashes" in {
      val map = Args(Array("-one", "1", "--two", "2", "---three", "3"))
      map("three") must be_==("3")
      map("two") must be_==("2")
      map("one") must be_==("1")
    }
    "round trip to/from string" in {
      val a = Args("--you all every --body 1 2")
      a must be_==(Args(a.toString))
    }
  }
}
