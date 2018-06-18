(ns ziggurat.messaging.util-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.util :refer [prefixed-queue-name]]))

(deftest test-get-name-with-prefix-topic
  (testing "given a topic-entity and queue-name, it returns a string with topic-entity as the prefix of queue-name"
    (let [topic-entity    :topic
          queue-name      "queue_name"
          expected-string "topic_queue_name"]
      (is (= (prefixed-queue-name topic-entity queue-name) expected-string)))))
