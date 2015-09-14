package main

import (
  "github.com/julienschmidt/httprouter"
  "github.com/unrolled/render"
  "github.com/nicolasbarbe/kafka"
  "gopkg.in/mgo.v2"
  "net/http"
  "strings"
  "log"
  "os"
)

/** Constants **/

const (
  answersCollection     = "answers"
  answersTopic          = "answers"
  answerPosted          = "AnswerPosted"
)

var (
    brokers               = strings.Split(os.Getenv("KAFKA_BROKERS"), ",")  
    usersQueriesCS        = os.Getenv("USERS_QUERIES_CS")
)

/** Main **/

func main() {

  // create kafka producer
  var producer = kafka.NewProducer(brokers)

  // create http renderer
  var renderer = render.New(render.Options{
      IndentJSON: true,
  })

  // create mongodb session
  session, err := mgo.Dial(os.Getenv("MONGODB_CS"))
  if err != nil {
    log.Fatal("Cannot connect to mongo server: %s", err)
  }

  mongo := session.DB(os.Getenv("MONGODB_DB"))

  // create controller
  controller := Controller {
    mongo    : mongo,
    producer : producer,
    renderer : renderer,
  }

  log.Println("Initializing consumers...")

  // listen to the creation of new users
  // usersConsumer := kafka.NewConsumer(brokers, usersTopic)
  // usersConsumer.Consume(controller.ConsumeUsers )

    // listen to the creation of new answers
  answersConsumer := kafka.NewConsumer(brokers, answersTopic)
  answersConsumer.Consume(controller.ConsumeAnswers )

  log.Println("Initializing routes...")

  // create routes
  router := httprouter.New()
  router.GET("/api/v1/answers", controller.ListAnswers)
  router.GET("/api/v1/answers/:id", controller.ShowAnswer)
  
  log.Println("Start listening...")

  log.Fatal(http.ListenAndServe(":8080", router))

  defer func() {
    producer.Close()
    session.Close()
    // usersConsumer.Close()
    answersConsumer.Close()
  }()
}