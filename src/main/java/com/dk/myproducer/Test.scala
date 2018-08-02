package com.dk.myproducer

class Test {


}

object Test{
    def main(args: Array[String]): Unit = {
//        for(i <- 1 to 10){
//            println(i)
//        }
        val i = Array(1,2,3,4,5)
        println(i)
        for(j <- i){
            println(j)
        }
    }
}