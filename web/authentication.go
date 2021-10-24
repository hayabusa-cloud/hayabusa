package web

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/labstack/gommon/random"
)

const (
	// PermissionGuest is guest(unauthorized)
	PermissionGuest = 0
	// PermissionNormal is normal
	PermissionNormal = 1
)

// Authentication represents basic authentication information
type Authentication struct {
	UserID      string `json:"userId"      bson:"user_id"        sql:"user_id"`
	password    string `json:"-"    	   bson:"password"       sql:"password"`
	AccessToken string `json:"accessToken" bson:"access_token"   sql:"access_token"`
	Permission  uint8  `json:"permission"  bson:"permission"     sql:"permission"`
}

const userIDCharset = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvexyz0123456789"

func NewAuthentication(l ...int) *Authentication {
	var length = 12
	if len(l) > 0 && l[0] >= 6 {
		length = l[0]
	}
	var auth = &Authentication{
		UserID:     random.String(uint8(length), userIDCharset),
		Permission: PermissionGuest,
	}
	auth.randomize()
	return auth
}

func AuthenticationFromAccessToken(token string) (auth *Authentication) {
	token = strings.TrimPrefix(token, "Bearer ")
	bytes, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil
	}
	var decodedToken = string(bytes)
	var args = strings.Split(decodedToken, "#")
	if len(args) != 2 {
		return nil
	}
	auth = &Authentication{
		UserID:      args[0],
		password:    args[1],
		AccessToken: token,
		Permission:  PermissionGuest,
	}
	return auth
}

func (auth *Authentication) randomize() {
	auth.password = random.String(53, userIDCharset)
	token := fmt.Sprintf("%s#%s", auth.UserID, auth.password)
	auth.AccessToken = base64.StdEncoding.EncodeToString([]byte(token))
}

// ID returns user ID
func (auth *Authentication) ID() string {
	return auth.UserID
}

// HasPermission returns true if user has given permission level
func (auth *Authentication) HasPermission(perm uint8) bool {
	return auth.Permission >= perm
}
