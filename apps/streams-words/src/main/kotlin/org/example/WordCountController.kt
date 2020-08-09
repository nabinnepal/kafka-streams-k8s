package org.example

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/state")
class WordCountController(
    private val wordCountService: WordCountService
) {
    @GetMapping("/count/{word}")
    fun getWordCountFromStore(
        @PathVariable("word") word: String
    ): ResponseEntity<Pair<String, Long>> {
        return wordCountService.getCountFromStore(word)?.let {
                result -> ResponseEntity.ok(result)
        } ?: ResponseEntity.notFound().build()
    }

    @GetMapping("/count/{word}/{from}/{to}")
    fun getWordCountFromWindowStore(
        @PathVariable("word") word: String,
        @PathVariable("from") from: Long,
        @PathVariable("to") to: Long
    ): ResponseEntity<List<Pair<String, Long>>> {
        return wordCountService.getCountFromWindowStore(word, from, to).let {
                result -> ResponseEntity.ok(result)
        }
    }
}