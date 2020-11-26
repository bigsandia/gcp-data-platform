package org.dataplatform.dataloader.input

import spock.lang.Specification

class InputMessageUtilsTest extends Specification {

    def "fromJson should transform a JSON String to an instance of InputMessage"() {
        given:
        def file = new File(getClass().getResource('/test_full_event.json').toURI())
        String json = file.getText("UTF-8")

        when:
        InputMessage inputMessage = InputMessageUtils.fromJson(json)

        then:
        inputMessage.message.data == "ewogICJraW5kIjogInN0b3JhZ2Ujb2JqZWN0IiwKICAiaWQiOiAiYW5vdGhlci1kYXRhLXBsYXRmb3JtLXJhdy0xL3Rlc3RfZnVsbF8xLmNzdi8xNjA2NDAzMDc4MTg4MDI4IiwKICAic2VsZkxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vc3RvcmFnZS92MS9iL2Fub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMS9vL3Rlc3RfZnVsbF8xLmNzdiIsCiAgIm5hbWUiOiAidGVzdF9mdWxsXzEuY3N2IiwKICAiYnVja2V0IjogImFub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMSIsCiAgImdlbmVyYXRpb24iOiAiMTYwNjQwMzA3ODE4ODAyOCIsCiAgIm1ldGFnZW5lcmF0aW9uIjogIjEiLAogICJjb250ZW50VHlwZSI6ICJ0ZXh0L2NzdiIsCiAgInRpbWVDcmVhdGVkIjogIjIwMjAtMTEtMjZUMTU6MDQ6MzguMTg3WiIsCiAgInVwZGF0ZWQiOiAiMjAyMC0xMS0yNlQxNTowNDozOC4xODdaIiwKICAic3RvcmFnZUNsYXNzIjogIlNUQU5EQVJEIiwKICAidGltZVN0b3JhZ2VDbGFzc1VwZGF0ZWQiOiAiMjAyMC0xMS0yNlQxNTowNDozOC4xODdaIiwKICAic2l6ZSI6ICI1IiwKICAibWQ1SGFzaCI6ICJ3eXNnVjdtOVlzcW9OVGhqUmhkNU5RPT0iLAogICJtZWRpYUxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vZG93bmxvYWQvc3RvcmFnZS92MS9iL2Fub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMS9vL3Rlc3RfZnVsbF8xLmNzdj9nZW5lcmF0aW9uPTE2MDY0MDMwNzgxODgwMjgmYWx0PW1lZGlhIiwKICAiY3JjMzJjIjogInVUQThCdz09IiwKICAiZXRhZyI6ICJDUHpYM2VhOW9PMENFQUU9Igp9Cg=="
    }
}