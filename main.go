package main

// docker run --name pro0neo4j -p7474:7474 -p7687:7687 -d -v $HOME/neo4j/data:/data --env NEO4J_AUTH=neo4j/neo4j neo4j:latest
import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/valyala/fastjson"
)

var wg sync.WaitGroup

func main() {
	uri := flag.String("uri", "bolt://127.0.0.1:7687", "URL of neo4j server Default: bolt://127.0.0.1:7687")
	username := flag.String("username", "neo4j", "Username for neo4j server Default: neo4j")
	password := flag.String("password", "test", "Password for neo4j server Default: test")

	pr0username := flag.String("pr0username", "", "Username for Pr0Gramm")
	pr0password := flag.String("pr0password", "", "Password for Pr0Gramm")
	flag.Parse()
	if !strings.HasPrefix(*uri, "bolt://") {
		*uri = "bolt://" + *uri
	}

	fmt.Println("Connecting to neo4j server at: ", *uri, " ...")

	// Connect to the database
	driver, err := neo4j.NewDriver(*uri, neo4j.BasicAuth(*username, *password, ""))
	if err != nil {
		panic(err)
	}
	defer driver.Close()

	jar, err := cookiejar.New(nil)
	if err != nil {
		panic(err)
	}

	client := &http.Client{
		Jar: jar,
	}
	loginUser(*pr0username, *pr0password, client)
	err = setupDB(driver)
	if err != nil {
		panic(err)
	}

	latestID := getLatestPostID(client)
	nextPosts, latestID := getNextXPosts(latestID, client)

	for latestID > 2 {
		start := time.Now()
		for _, post := range nextPosts {

			tags, comments := getTagsAndCommentsOfPost(post.Id, client)

			authorData, badges := getUserDetails(post.creatorname, client)
			wg.Add(1)
			go registerPostInDB(driver, authorData, post, tags, comments, badges)

		}
		wg.Wait()
		nextPosts, latestID = getNextXPosts(latestID, client)
		elapsed := time.Since(start)
		fmt.Printf("Time %s for %d posts\n", elapsed, len(nextPosts))
	}
}

func getUserDetails(name string, client *http.Client) (User, []Badge) {
	url := "https://pr0gramm.com/api/profile/info?name=" + name + "&flags=15"
	resp, err := client.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		panic(err)
	}

	var user User
	user.Id = v.GetInt("user", "id")
	user.Name = string(v.GetStringBytes("user", "name"))
	user.Registered = v.GetInt("user", "registered")
	user.Score = v.GetInt("user", "score")

	var badges []Badge
	for _, value := range v.GetArray("badges") {
		var badge Badge
		badge.Name = string(value.GetStringBytes("image"))
		badges = append(badges, badge)
	}
	return user, badges
}

func getLatestPostID(client *http.Client) int {
	url := "https://pr0gramm.com/api/items/get?flags=15"
	resp, err := client.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		panic(err)
	}
	return v.GetInt("items", "0", "id")
}

func getNextXPosts(startID int, client *http.Client) ([]Post, int) {
	url := "https://pr0gramm.com/api/items/get?older=" + strconv.Itoa(startID) + "&flags=15"
	resp, err := client.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		panic(err)
	}
	var posts []Post
	// Get array of json posts
	for _, value := range v.GetArray("items") {
		var post Post
		post.Id = value.GetInt("id")
		post.Up = value.GetInt("up")
		post.Down = value.GetInt("down")
		post.Created = value.GetInt("created")
		post.Width = value.GetInt("width")
		post.Height = value.GetInt("height")
		post.Audio = value.GetInt("audio")
		post.Flags = value.GetInt("flags")
		post.Url = string(value.GetStringBytes("image"))
		post.userID = value.GetInt("userId")
		post.creatorname = string(value.GetStringBytes("user"))
		posts = append(posts, post)

	}
	return posts, posts[len(posts)-1].Id
}

func getTagsAndCommentsOfPost(id int, client *http.Client) ([]Tag, []Comment) {
	url := "https://pr0gramm.com/api/items/info?itemId=" + strconv.Itoa(id)
	resp, err := client.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		panic(err)
	}
	var tags []Tag
	var comments []Comment
	// Get array of json tags
	for _, value := range v.GetArray("tags") {
		var tag Tag
		tag.Name = string(value.GetStringBytes("tag"))
		tag.Confidence = value.GetFloat64("confidence")
		tags = append(tags, tag)
	}
	// Get array of json comments
	for _, value := range v.GetArray("comments") {
		var comment Comment
		comment.ID = value.GetInt("id")
		comment.Up = value.GetInt("up")
		comment.Down = value.GetInt("down")
		comment.Created = value.GetInt("created")
		comment.Content = string(value.GetStringBytes("content"))
		comment.parentId = value.GetInt("parent")
		comments = append(comments, comment)
	}
	return tags, comments
}

func loginUser(username string, password string, client *http.Client) {
	token, captchaSolution := solveCaptcha(client)

	data := url.Values{
		"name":     {username},
		"password": {password},
		"token":    {token},
		"captcha":  {captchaSolution},
	}
	url := "https://pr0gramm.com/api/user/login"
	resp, err := http.PostForm(url, data)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))

	client.Jar.SetCookies(resp.Request.URL, resp.Cookies())
	return

}

func solveCaptcha(client *http.Client) (string, string) {
	url := "https://pr0gramm.com/api/user/captcha"
	resp, err := client.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v.GetStringBytes("captcha")))
	// Get user input
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter captcha solution: ")
	text, _ := reader.ReadString('\n')
	// remove \n from text
	text = strings.Replace(text, "\n", "", -1)

	return string(v.GetStringBytes("token")), text
}

func registerPostInDB(driver neo4j.Driver, authorData User, post Post, tags []Tag, comments []Comment, badges []Badge) {
	defer wg.Done()
	err := insertUser(authorData, driver)
	if err != nil {
		panic(err)
	}
	err = insertPost(post, driver)
	if err != nil {
		panic(err)
	}
	for _, tag := range tags {

		err = insertTag(tag, driver)
		if err != nil {
			panic(err)
		}
		err = connectTagToPost(tag, post, driver)
		if err != nil {
			panic(err)
		}

	}
	for _, comment := range comments {
		err = insertComment(comment, driver)
		if err != nil {
			panic(err)
		}
		err = connectCommentToPost(comment, post, driver)
		if err != nil {
			panic(err)
		}

	}
	for _, badge := range badges {
		err = insertBadge(badge, driver)
		if err != nil {
			panic(err)
		}
		err = connectBadgeToUser(badge, authorData, driver)
		if err != nil {
			panic(err)
		}

	}

	for _, comment := range comments {
		err = connectCommentToUser(comment, authorData, driver)
		if err != nil {
			panic(err)
		}
		for _, comment2 := range comments {

			if comment.parentId == comment2.ID {
				err = connectCommentToComment(comment, comment2, driver)
				if err != nil {
					panic(err)
				}
			}
			if comment.ID == comment2.parentId {
				err = connectCommentToComment(comment2, comment, driver)
				if err != nil {
					panic(err)
				}
			}

		}

	}

}

func setupDB(driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "CREATE CONSTRAINT FOR (n:Tag) REQUIRE n.name IS UNIQUE"
		result, err := tx.Run(query, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	_, err = session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "CREATE CONSTRAINT FOR (n:Badge) REQUIRE n.name IS UNIQUE"
		result, err := tx.Run(query, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	_, err = session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "CREATE CONSTRAINT FOR (n:Comment) REQUIRE n.pr0id IS UNIQUE"
		result, err := tx.Run(query, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})

	_, err = session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "CREATE CONSTRAINT FOR (n:User) REQUIRE n.pr0id IS UNIQUE"
		result, err := tx.Run(query, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	_, err = session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "CREATE CONSTRAINT FOR (n:Post) REQUIRE n.pr0id IS UNIQUE"
		result, err := tx.Run(query, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err

}

type Tag struct {
	Name       string
	Confidence float64
}

func insertTag(tag Tag, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MERGE (a:Tag { name: $name }) return a"
		result, err := tx.Run(query, map[string]interface{}{
			"name": tag.Name,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}

type Badge struct {
	Name string
}

func insertBadge(badge Badge, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MERGE (a:Badge { name: $name }) return a"
		result, err := tx.Run(query, map[string]interface{}{
			"name": badge.Name,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}

type Comment struct {
	ID       int
	Up       int
	Down     int
	Created  int
	Content  string
	parentId int
}

func insertComment(comment Comment, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MERGE (a:Comment { pr0id: $id, up: $up, down: $down, created: $created, content: $content }) ON MATCH SET a.up = $up, a.down = $down return a"
		result, err := tx.Run(query, map[string]interface{}{
			"id":      comment.ID,
			"up":      comment.Up,
			"down":    comment.Down,
			"created": comment.Created,
			"content": comment.Content,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}

type User struct {
	Id         int
	Name       string
	Registered int
	Score      int
}

func insertUser(user User, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MERGE (a:User {pr0id: $id,name: $username,  registered: $registered, score: $score}) ON MATCH SET a.score = $score  return a"
		result, err := tx.Run(query, map[string]interface{}{
			"id":         user.Id,
			"username":   user.Name,
			"registered": user.Registered,
			"score":      user.Score,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}

type Post struct {
	Id          int
	Up          int
	Down        int
	Created     int
	Width       int
	Height      int
	Audio       int
	Flags       int
	Url         string
	userID      int
	creatorname string
}

func insertPost(post Post, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MERGE (a:Post { pr0id: $id, up: $up, down: $down, created: $created, width: $width, height: $height, audio: $audio, flags: $flags, url: $Url }) ON MATCH SET a.up = $up, a.down = $down return a"
		result, err := tx.Run(query, map[string]interface{}{
			"id":      post.Id,
			"up":      post.Up,
			"down":    post.Down,
			"created": post.Created,
			"width":   post.Width,
			"height":  post.Height,
			"audio":   post.Audio,
			"flags":   post.Flags,
			"Url":     post.Url,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}

func connectBadgeToUser(badge Badge, user User, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MATCH (u:User {pr0id: $userId}), (b:Badge {name: $name}) MERGE (u)-[c:OwnsBadge]->(b) return u,b,c"
		result, err := tx.Run(query, map[string]interface{}{
			"userId": user.Id,
			"name":   badge.Name,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}
func connectTagToPost(tag Tag, post Post, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MATCH (p:Post {pr0id: $postId}), (t:Tag {name: $name}) MERGE (p)-[c:HasTag]->(t) return p,t,c"
		result, err := tx.Run(query, map[string]interface{}{
			"postId": post.Id,
			"name":   tag.Name,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}
func connectCommentToPost(comment Comment, post Post, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MATCH (p:Post {pr0id: $postId}), (b:Comment {pr0id: $id}) MERGE (p)-[c:HasComment]->(b) return p,b,c"
		result, err := tx.Run(query, map[string]interface{}{
			"postId": post.Id,
			"id":     comment.ID,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}
func connectCommentToUser(comment Comment, user User, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MATCH (u:User {pr0id: $userId}), (b:Comment {pr0id: $id}) MERGE (u)-[a:MadeComment]->(b) return a,b,u"
		result, err := tx.Run(query, map[string]interface{}{
			"userId": user.Id,
			"id":     comment.ID,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}
func connectCommentToComment(comment1 Comment, comment2 Comment, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		query := "MATCH (c1:Comment {pr0id: $id1}), (c2:Comment {pr0id: $id2}) MERGE (c1)-[a:IsParentFor]->(c2) return a,c1,c2"
		result, err := tx.Run(query, map[string]interface{}{
			"id1": comment1.ID,
			"id2": comment2.ID,
		})
		if err != nil {
			return nil, err
		}
		result.Consume()
		return nil, err
	})
	return err
}
