package org.example

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/state")
class WordCountController(
    private val wordCountService: WordCountService
) {
    @GetMapping("/count/{word}")
    fun getWordCountFromStore(
        @PathVariable("word") word: String
    ): Mono<WordCounter> {
        return wordCountService.getCountFromStore(word)
    }

    @GetMapping("/count/{word}/{from}/{to}")
    fun getWordCountFromWindowStore(
        @PathVariable("word") word: String,
        @PathVariable("from") from: Long,
        @PathVariable("to") to: Long
    ): Flux<WordCounter> {
        return wordCountService.getCountFromWindowStore(word, from, to)
    }
}