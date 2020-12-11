package org.dataplatform.dataloader.input

import com.google.api.services.storage.model.Notification
import spock.lang.Specification

class InputMessageConverterTest extends Specification {

    def "fromJson should transform a JSON String to an instance of InputMessage"() {
        given:
        def file = new File(getClass().getResource('/test_full_event.json').toURI())
        String json = file.getText("UTF-8")

        when:
        InputMessage inputMessage = InputMessageConverter.fromJson(json)

        then:
        inputMessage.message.data == "ewogICJraW5kIjogInN0b3JhZ2Ujb2JqZWN0IiwKICAiaWQiOiAiYW5vdGhlci1kYXRhLXBsYXRmb3JtLXJhdy0xL3Rlc3RfZnVsbF8xLmNzdi8xNjA2NDAzMDc4MTg4MDI4IiwKICAic2VsZkxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vc3RvcmFnZS92MS9iL2Fub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMS9vL3Rlc3RfZnVsbF8xLmNzdiIsCiAgIm5hbWUiOiAidGVzdF9mdWxsXzEuY3N2IiwKICAiYnVja2V0IjogImFub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMSIsCiAgImdlbmVyYXRpb24iOiAiMTYwNjQwMzA3ODE4ODAyOCIsCiAgIm1ldGFnZW5lcmF0aW9uIjogIjEiLAogICJjb250ZW50VHlwZSI6ICJ0ZXh0L2NzdiIsCiAgInRpbWVDcmVhdGVkIjogIjIwMjAtMTEtMjZUMTU6MDQ6MzguMTg3WiIsCiAgInVwZGF0ZWQiOiAiMjAyMC0xMS0yNlQxNTowNDozOC4xODdaIiwKICAic3RvcmFnZUNsYXNzIjogIlNUQU5EQVJEIiwKICAidGltZVN0b3JhZ2VDbGFzc1VwZGF0ZWQiOiAiMjAyMC0xMS0yNlQxNTowNDozOC4xODdaIiwKICAic2l6ZSI6ICI1IiwKICAibWQ1SGFzaCI6ICJ3eXNnVjdtOVlzcW9OVGhqUmhkNU5RPT0iLAogICJtZWRpYUxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vZG93bmxvYWQvc3RvcmFnZS92MS9iL2Fub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMS9vL3Rlc3RfZnVsbF8xLmNzdj9nZW5lcmF0aW9uPTE2MDY0MDMwNzgxODgwMjgmYWx0PW1lZGlhIiwKICAiY3JjMzJjIjogInVUQThCdz09IiwKICAiZXRhZyI6ICJDUHpYM2VhOW9PMENFQUU9Igp9Cg=="
    }

    def "Should extract Notification from InputMessage"() {
        given:
        String inputMessage = """{
  "deliveryAttempt": 37,
  "message": {
    "attributes": {
      "bucketId": "another-data-platform-raw-1",
      "eventTime": "2020-12-08T11:51:26.395668Z",
      "eventType": "OBJECT_FINALIZE",
      "notificationConfig": "projects/_/buckets/another-data-platform-raw-1/notificationConfigs/1",
      "objectGeneration": "1607428286395814",
      "objectId": "test_full_1.csv",
      "overwroteGeneration": "1607423100840561",
      "payloadFormat": "JSON_API_V1"
    },
    "data": "ewogICJraW5kIjogInN0b3JhZ2Ujb2JqZWN0IiwKICAiaWQiOiAiYW5vdGhlci1kYXRhLXBsYXRmb3JtLXJhdy0xL3Rlc3RfZnVsbF8xLmNzdi8xNjA3NDI4Mjg2Mzk1ODE0IiwKICAic2VsZkxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vc3RvcmFnZS92MS9iL2Fub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMS9vL3Rlc3RfZnVsbF8xLmNzdiIsCiAgIm5hbWUiOiAidGVzdF9mdWxsXzEuY3N2IiwKICAiYnVja2V0IjogImFub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMSIsCiAgImdlbmVyYXRpb24iOiAiMTYwNzQyODI4NjM5NTgxNCIsCiAgIm1ldGFnZW5lcmF0aW9uIjogIjEiLAogICJjb250ZW50VHlwZSI6ICJ0ZXh0L2NzdiIsCiAgInRpbWVDcmVhdGVkIjogIjIwMjAtMTItMDhUMTE6NTE6MjYuMzk1WiIsCiAgInVwZGF0ZWQiOiAiMjAyMC0xMi0wOFQxMTo1MToyNi4zOTVaIiwKICAic3RvcmFnZUNsYXNzIjogIlNUQU5EQVJEIiwKICAidGltZVN0b3JhZ2VDbGFzc1VwZGF0ZWQiOiAiMjAyMC0xMi0wOFQxMTo1MToyNi4zOTVaIiwKICAic2l6ZSI6ICI1IiwKICAibWQ1SGFzaCI6ICJ3eXNnVjdtOVlzcW9OVGhqUmhkNU5RPT0iLAogICJtZWRpYUxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vZG93bmxvYWQvc3RvcmFnZS92MS9iL2Fub3RoZXItZGF0YS1wbGF0Zm9ybS1yYXctMS9vL3Rlc3RfZnVsbF8xLmNzdj9nZW5lcmF0aW9uPTE2MDc0MjgyODYzOTU4MTQmYWx0PW1lZGlhIiwKICAiY3JjMzJjIjogInVUQThCdz09IiwKICAiZXRhZyI6ICJDS2JyaklDcHZ1MENFQUU9Igp9Cg==",
    "messageId": "1670753549413554",
    "message_id": "1670753549413554",
    "publishTime": "2020-12-08T11:51:26.744Z",
    "publish_time": "2020-12-08T11:51:26.744Z"
  },
  "subscription": "projects/another-data-platform/subscriptions/data-loader"
}
"""

        when:
        Notification notification = InputMessageConverter.extractNotificationMessage(inputMessage)

        then:
        notification.get("bucket") == "another-data-platform-raw-1"
        notification.getId() == "another-data-platform-raw-1/test_full_1.csv/1607428286395814"
        notification.get("name") == "test_full_1.csv"
    }
}
