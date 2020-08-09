package org.example

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class WordsCountApplication

fun main(args: Array<String>) {
    runApplication<WordsCountApplication>(*args)
}