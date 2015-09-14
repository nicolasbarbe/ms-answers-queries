package main

import ( 
        "github.com/julienschmidt/httprouter"
        "github.com/unrolled/render"
        "gopkg.in/mgo.v2"
        "github.com/nicolasbarbe/kafka"
        "encoding/json" 
        "net/http"
        "strconv"
        "io/ioutil"
        "time"
        "fmt"
        "log"
)


/** Types **/

type NormalizedAnswer struct {
  Id            string             `json:"id"            bson:"_id"`
  Content       string             `json:"content"       bson:"content"`
  Author        string             `json:"author"        bson:"author"`
  CreatedAt     time.Time          `json:"createdAt"     bson:"createdAt"`
  Discussion    string             `json:"discussion"    bson:"discussion"`
}

type DenormalizedAnswer struct {
  Id            string             `json:"id"            bson:"_id"`
  Content       string             `json:"content"       bson:"content"`
  Author        string             `json:"author"        bson:"author"`
  CreatedAt     time.Time          `json:"createdAt"     bson:"createdAt"`
  Discussion    string             `json:"discussion"    bson:"discussion"`
}

type DenormalizedUser struct {
  Id            string            `json:"id"            bson:"_id"`
  FirstName     string            `json:"firstName"     bson:"firstName"`
  LastName      string            `json:"lastName"      bson:"lastName"`
  MemberSince   time.Time         `json:"memberSince"   bson:"memberSince"`
}


// Controller embeds the logic of the microservice
type Controller struct {
  mongo         *mgo.Database
  producer      *kafka.Producer
  renderer      *render.Render
}

func (this *Controller) ListAnswers(response http.ResponseWriter, request *http.Request, params httprouter.Params ) {
  var answers []DenormalizedAnswer
  if err := this.mongo.C(answersCollection).Find(nil).Limit(100).All(&answers) ; err != nil {
    log.Print(err)
  }
  this.renderer.JSON(response, http.StatusOK, answers)
}

func (this *Controller) ShowAnswer(response http.ResponseWriter, request *http.Request, params httprouter.Params ) {
  var answer DenormalizedAnswer

  if err := this.mongo.C(answersCollection).FindId(params.ByName("id")).One(&answer) ; err != nil {
    this.renderer.JSON(response, http.StatusNotFound, "Answer not found")
    return
  }
  this.renderer.JSON(response, http.StatusOK, answer)
}

func (this *Controller) ConsumeAnswers(message []byte) {
  idx, _    := strconv.Atoi(string(message[:2]))
  eventType := string(message[2:idx+2])
  body      := message[idx+2:]

  if eventType != answerPosted {
    log.Printf("Message with type %v is ignored. Type %v was expected", eventType, answerPosted)
    return
  }

  // unmarshal answer from event body
  var normalizedAnswer NormalizedAnswer
  if err := json.Unmarshal(body, &normalizedAnswer); err != nil {
    log.Print("Cannot unmarshal answer")
    return
  }

  // fetch user details
  var user DenormalizedUser 
  if err := this.fetchResource(usersQueriesCS, normalizedAnswer.Author, &user) ; err != nil {
    log.Print("Cannot fetch resource: ", err)
    return
  }

  // create internal representation
  denormalizedAnswer := DenormalizedAnswer {
   Id           : normalizedAnswer.Id,            
   Content      : normalizedAnswer.Content,      
   Author       : fmt.Sprintf("%v %v", user.FirstName, user.LastName), 
   CreatedAt    : normalizedAnswer.CreatedAt, 
   Discussion   : normalizedAnswer.Discussion,
  }

  // save answer
  if err := this.mongo.C(answersCollection).Insert(denormalizedAnswer) ; err != nil {
    log.Printf("Cannot save document in collection %s : %s", answersCollection, err)
    return
  }
}


func (this *Controller) fetchResource(cs string, id string, resource interface{}) error {
  resp, err := http.Get("http://" + cs + "/api/v1/users/" + id)

  if err != nil {
    return fmt.Errorf("Cannot retrieve resource with identifier %v from service %v", id, cs)
  }

  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
      return fmt.Errorf("Cannot read resource with identifier %v from service %v", id, cs)
  }
  
  // unmarshall resource
  if err := json.Unmarshal(body, resource); err != nil {
    return fmt.Errorf("Cannot unmarshal resources with identifier %v", id)
  }
  return nil
}


