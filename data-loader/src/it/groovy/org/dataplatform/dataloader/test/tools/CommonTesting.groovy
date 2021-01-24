package org.dataplatform.dataloader.test.tools

trait CommonTesting {

     byte[] getResourceAsBytes(String resourcePath) {
        new File(getClass().getResource("/$resourcePath").toURI()).bytes
    }

}
