package main

// docker run --name pro0neo4j -p7474:7474 -p7687:7687 -d -v $HOME/neo4j/data:/data --env NEO4J_AUTH=neo4j/neo4j neo4j:latest
import (
	"bufio"
	"errors"
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

	mode := flag.String("mode", "online", "start at online first or offline (db) first")
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

	/*err = loginUser(*pr0username, *pr0password, client)
	if err != nil {
		panic(err)
	}*/
	// read cookie out of .cookie and instert into cookiejar for pr0gramm.com
	cookie, err := ioutil.ReadFile(".cookie")
	if err != nil {
		panic(err)
	}
	cookieURL, err := url.Parse("https://pr0gramm.com")
	if err != nil {
		panic(err)
	}
	jar.SetCookies(cookieURL, []*http.Cookie{{Name: "me", Value: string(cookie)}})
	setupDB(driver)
	var latestID int
	if *mode == "online" {
		latestID = getLatestPostID(client)
		fmt.Println("Starting online with: ", latestID)
	} else {
		latestID, err = getlatestPostIDFromDB(driver)
		if err != nil {
			panic(err)
		}
		fmt.Println("Starting offline with: ", latestID)
	}

	nextPosts, latestID, err := getNextXPosts(latestID, client)
	for err != nil {
		fmt.Println(err)
		time.Sleep(10 * time.Minute)
		nextPosts, latestID, err = getNextXPosts(latestID, client)
	}

	for latestID > 2 {
		start := time.Now()
		for _, post := range nextPosts {

			tags, comments, err := getTagsAndCommentsOfPost(post.Id, client)
			for err != nil {
				fmt.Println(err)
				time.Sleep(10 * time.Minute)
				tags, comments, err = getTagsAndCommentsOfPost(post.Id, client)
			}

			authorData, badges, err := getUserDetails(post.creatorname, client)
			for err != nil {
				fmt.Println(err)
				time.Sleep(10 * time.Minute)
				authorData, badges, err = getUserDetails(post.creatorname, client)
			}

			wg.Add(1)
			go registerPostInDB(driver, authorData, post, tags, comments, badges, client)

		}
		wg.Wait()
		nextPosts, latestID, err = getNextXPosts(latestID, client)
		for err != nil {
			fmt.Println(err)
			time.Sleep(10 * time.Minute)
			nextPosts, latestID, err = getNextXPosts(latestID, client)
		}
		elapsed := time.Since(start)
		dt := time.Now()
		timeFormat := "02.01.2001 15:04"
		fmt.Printf("\r[%s] [Time: %s] [Amount: %d] [ID: %d] [Created: %s]", dt.Format(timeFormat), elapsed, len(nextPosts), latestID, time.Unix(int64(nextPosts[0].Created), 0).Format(timeFormat))
	}
}

func getUserDetails(name string, client *http.Client) (User, []Badge, error) {
	url := "https://pr0gramm.com/api/profile/info?name=" + name + "&flags=15"
	resp, err := client.Get(url)

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	var p fastjson.Parser
	v, err := p.ParseBytes(body)

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
	return user, badges, err
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

func getNextXPosts(startID int, client *http.Client) ([]Post, int, error) {
	url := "https://pr0gramm.com/api/items/get?older=" + strconv.Itoa(startID) + "&flags=15"
	resp, err := client.Get(url)
	if err != nil {
		return nil, startID, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, startID, err
	}

	if resp.StatusCode != 200 {
		return nil, startID, errors.New(string(body))
	}

	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		return nil, startID, err
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

	return posts, posts[len(posts)-1].Id, err
}

func getTagsAndCommentsOfPost(id int, client *http.Client) ([]Tag, []Comment, error) {
	url := "https://pr0gramm.com/api/items/info?itemId=" + strconv.Itoa(id)
	resp, err := client.Get(url)

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	var p fastjson.Parser
	v, err := p.ParseBytes(body)

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
		comment.Name = string(value.GetStringBytes("name"))
		comments = append(comments, comment)
	}
	return tags, comments, err
}

func loginUser(username string, password string, client *http.Client) error {
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
	if resp.StatusCode != 200 {
		return errors.New(string(body))
	}

	client.Jar.SetCookies(resp.Request.URL, resp.Cookies())
	return nil

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

func registerPostInDB(driver neo4j.Driver, authorData User, post Post, tags []Tag, comments []Comment, badges []Badge, client *http.Client) {
	defer wg.Done()
	err := insertUser(authorData, driver)
	if err != nil {
		panic(err)
	}
	err = insertPost(post, driver)
	if err != nil {
		panic(err)
	}
	err = connectPostToUser(post, authorData, driver)
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
		// Get user data of comment author
		comentAuthorId, err := getUserIdByNameFromDB(comment.Name, driver, client)
		if err != nil {
			panic(err)
		}

		err = connectCommentToUser(comment, comentAuthorId, driver)
		if err != nil {
			panic(err)
		}
		for _, comment2 := range comments {

			if comment.parentId == comment2.ID {
				err = connectCommentToComment(comment2, comment, driver)
				if err != nil {
					panic(err)
				}
			}
			if comment.ID == comment2.parentId {
				err = connectCommentToComment(comment, comment2, driver)
				if err != nil {
					panic(err)
				}
			}

		}

	}

}
func getlatestPostIDFromDB(driver neo4j.Driver) (int, error) {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close()

	result, err := session.Run("MATCH (p:Post) RETURN p ORDER BY p.pr0id ASC LIMIT 1", nil)
	if err != nil {
		return 0, err
	}
	var latestPostID int

	if result.Next() {
		record := result.Record()
		postAsNode := record.GetByIndex(0).(neo4j.Node)
		latestPostID = int(postAsNode.Props["pr0id"].(int64))

	}
	return latestPostID, nil
}

func getUserIdByNameFromDB(username string, driver neo4j.Driver, client *http.Client) (int, error) {
	//fmt.Println("Searching for user " + username)
	var userId int
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close()
	result, err := session.Run("MATCH (u:User) WHERE u.name = $username RETURN u", map[string]interface{}{"username": username})
	if err != nil {
		return 0, err
	}
	// check if results is empty
	if !result.Next() {
		userData, badges, err := getUserDetails(username, client)
		if err != nil {
			return 0, err
		}
		err = insertUser(userData, driver)
		if err != nil {
			return 0, err
		}
		for _, badge := range badges {
			err = insertBadge(badge, driver)
			if err != nil {
				return 0, err
			}
			err = connectBadgeToUser(badge, userData, driver)
			if err != nil {
				return 0, err
			}

		}
		userId = userData.Id

	} else {
		record := result.Record()
		userAsNode := record.GetByIndex(0).(neo4j.Node)
		userId = int(userAsNode.Props["pr0id"].(int64))
	}

	return userId, nil
}

func setupDB(driver neo4j.Driver) {

	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	query := "CALL db.constraints()"
	_, err := session.Run(query, map[string]interface{}{})
	if err != nil {
		fmt.Println("DB already setup")

	}
	query = "CREATE CONSTRAINT FOR (n:Tag) REQUIRE n.name IS UNIQUE"
	_, err = session.Run(query, map[string]interface{}{})
	if err != nil {
		fmt.Println("DB already setup")

	}
	query = "CREATE CONSTRAINT FOR (n:Badge) REQUIRE n.name IS UNIQUE"
	_, err = session.Run(query, map[string]interface{}{})
	if err != nil {
		fmt.Println("DB already setup")

	}
	query = "CREATE CONSTRAINT FOR (n:Comment) REQUIRE n.pr0id IS UNIQUE"
	_, err = session.Run(query, map[string]interface{}{})
	if err != nil {
		fmt.Println("DB already setup")

	}
	query = "CREATE CONSTRAINT FOR (n:User) REQUIRE n.pr0id IS UNIQUE"
	_, err = session.Run(query, map[string]interface{}{})
	if err != nil {
		fmt.Println("DB already setup")

	}
	query = "CREATE CONSTRAINT FOR (n:Post) REQUIRE n.pr0id IS UNIQUE"
	_, err = session.Run(query, map[string]interface{}{})
	if err != nil {
		fmt.Println("DB already setup")

	}

}

type Tag struct {
	Name       string
	Confidence float64
}

func insertTag(tag Tag, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MERGE (a:Tag { name: $name }) return a"
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{
		"name": tag.Name,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err
}

type Badge struct {
	Name string
}

func insertBadge(badge Badge, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MERGE (a:Badge { name: $name }) return a"
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{
		"name": badge.Name,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err
}

type Comment struct {
	ID       int
	Up       int
	Down     int
	Created  int
	Content  string
	parentId int
	Name     string
}

func insertComment(comment Comment, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MERGE (a:Comment { pr0id: $id, up: $up, down: $down, created: $created, content: $content }) ON MATCH SET a.up = $up, a.down = $down return a"
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{
		"id":      comment.ID,
		"up":      comment.Up,
		"down":    comment.Down,
		"created": comment.Created,
		"content": comment.Content,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
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
	query := "MERGE (a:User {pr0id: $id,name: $username,  registered: $registered, score: $score}) ON MATCH SET a.score = $score, a.name = $username, a.registered = $registered, a.pr0id = $id  return a"
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{
		"id":         user.Id,
		"username":   user.Name,
		"registered": user.Registered,
		"score":      user.Score,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
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
	query := "MERGE (a:Post { pr0id: $id, up: $up, down: $down, created: $created, width: $width, height: $height, audio: $audio, flags: $flags, url: $Url }) ON MATCH SET a.up = $up, a.down = $down return a"
	result, err := session.Run(query, map[string]interface{}{
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
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err
}

func connectBadgeToUser(badge Badge, user User, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MATCH (u:User {pr0id: $userId}), (b:Badge {name: $name}) MERGE (u)-[c:OwnsBadge]->(b) return u,b,c"
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{
		"userId": user.Id,
		"name":   badge.Name,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err
}
func connectTagToPost(tag Tag, post Post, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MATCH (p:Post {pr0id: $postId}), (t:Tag {name: $name}) MERGE (p)-[c:HasTag]->(t) return p,t,c"
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{
		"postId": post.Id,
		"name":   tag.Name,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err
}
func connectCommentToPost(comment Comment, post Post, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MATCH (p:Post {pr0id: $postId}), (b:Comment {pr0id: $id}) MERGE (p)-[c:HasComment]->(b) return p,b,c"
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{
		"postId": post.Id,
		"id":     comment.ID,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err
}
func connectCommentToUser(comment Comment, userId int, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MATCH (u:User {pr0id: $userId}), (b:Comment {pr0id: $id}) MERGE (u)-[a:MadeComment]->(b) return a,b,u"
	defer session.Close()
	result, err := session.Run(query, map[string]interface{}{
		"userId": userId,
		"id":     comment.ID,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err

}
func connectCommentToComment(comment1 Comment, comment2 Comment, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MATCH (c1:Comment {pr0id: $id1}), (c2:Comment {pr0id: $id2}) MERGE (c1)-[a:IsParentFor]->(c2) return a,c1,c2"
	defer session.Close()

	result, err := session.Run(query, map[string]interface{}{
		"id1": comment1.ID,
		"id2": comment2.ID,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err

}

func connectPostToUser(post Post, user User, driver neo4j.Driver) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	query := "MATCH (u:User {pr0id: $userId}), (p:Post {pr0id: $id}) MERGE (u)-[a:MadePost]->(p) return a,p,u"
	defer session.Close()

	result, err := session.Run(query, map[string]interface{}{
		"userId": user.Id,
		"id":     post.Id,
	})

	if err != nil {
		if strings.Contains(err.Error(), "Neo.ClientError.Schema.ConstraintValidationFailed") {
			return nil
		}
		return err
	}
	result.Consume()
	return err

}
