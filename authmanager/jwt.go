package authmanager

import (
	"time"

	"github.com/Crearosoft/corelib/loggermanager"

	jwt "github.com/dgrijalva/jwt-go"
)

// GlobalJWTKey - signature key
var GlobalJWTKey string

var keyFunc = func(key string) jwt.Keyfunc {
	return func(*jwt.Token) (interface{}, error) {
		return []byte(key), nil
	}
}

// Options struct
type Options struct {
	Username  string        `json:"username"`
	ExpiresAt time.Duration `json:"expiresAt"`
}

// Claims -payload storing in jwt token
type Claims struct {
	Username string `json:"username"`
	jwt.StandardClaims
}

// generate jwt token with payload and signature key
func generate(claims Claims, key string) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
}

// GenerateToken -with claims
func GenerateToken(loginID string, ExpiresAt int64) (string, error) {
	claims := Claims{
		Username: loginID,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: ExpiresAt,
		},
	}
	return generate(claims, GlobalJWTKey)
	// loggermanager.LogInfo(claims)
}

func decode(token *jwt.Token, err error) (jwt.MapClaims, error) {

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		// loggermanager.LogError("Error while parsing claims to MapClaims")
		return nil, loggermanager.Wrap("Error while parsing claims")
		// return nil, ok
	}

	return claims, nil
}

// DecodeJWTToken - decode token
func DecodeJWTToken(token string) (jwt.MapClaims, error) {
	return decode(jwt.Parse(token, keyFunc(GlobalJWTKey)))
}
