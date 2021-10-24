package utils

import (
	"bytes"
	"encoding/base64"
	"github.com/labstack/gommon/random"
	"math/rand"
)

type Credential struct {
	AccessKey    []byte
	accessSecret []byte
	pred, next   *Credential
}

type CredentialChain = *Credential

const (
	accessKeyCharset              = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvexyz0123456789"
	credentialDefaultSecretLength = 36
)

func NewCredentialFromKnownAccessKey(accessKey []byte, secretLength ...int) (credential *Credential) {
	credential = &Credential{AccessKey: accessKey}
	var l = credentialDefaultSecretLength
	if len(secretLength) > 0 {
		l = secretLength[0]
	}
	credential.accessSecret = make([]byte, l)
	if n, err := rand.Read(credential.accessSecret); err != nil {
		return nil
	} else if n < l {
		return nil
	}
	return
}

func NewCredentialFromKnownStringAccessKey(accessKey string, secretLength ...int) (credential *Credential) {
	return NewCredentialFromKnownAccessKey([]byte(accessKey), secretLength...)
}
func NewCredentialFromKnownAccessKeyChain(accessKey ...string) (credential *Credential) {
	credential = NewCredentialFromKnownStringAccessKey(accessKey[0])
	for i := 1; i < len(accessKey); i++ {
		credential.Attach(NewCredentialFromKnownStringAccessKey(accessKey[i]))
	}
	return credential
}

func (credential *Credential) Clone() *Credential {
	var pred, current *Credential = nil, credential
	// seek to head
	for current.pred != nil {
		current = current.pred
	}
	// copy
	for current != nil {
		newCred := &Credential{
			AccessKey:    current.AccessKey,
			accessSecret: current.accessSecret,
			pred:         pred,
		}
		if pred != nil {
			pred.next = newCred
		}

		pred = newCred
		current = current.next
	}
	// return head
	for pred.pred != nil {
		pred = pred.pred
	}
	return pred
}

func (credential *Credential) ID() []byte {
	return credential.AccessKey
}

func (credential *Credential) StringID() string {
	return string(credential.AccessKey)
}

func (credential *Credential) Token() []byte {
	var convertFn func(credential *Credential, secret []byte) []byte
	convertFn = func(credential *Credential, secret []byte) []byte {
		var joined []byte
		if credential == nil {
			joined = secret
		} else {
			joined = bytes.Join([][]byte{credential.AccessKey, convertFn(credential.next, credential.accessSecret)}, []byte{0x3a})
		}
		var encoded = make([]byte, ((len(joined)+2)/3)*4)
		base64.StdEncoding.Encode(encoded, joined)
		return encoded
	}
	return convertFn(credential, credential.accessSecret)
}

func (credential *Credential) StringToken() string {
	return string(credential.Token())
}

func (credential *Credential) AccessKeys() [][]byte {
	var current, chain = credential, make([][]byte, 0, 4)
	for current != nil {
		chain = append(chain, current.AccessKey)
		current = current.next
	}
	return chain
}

func (credential *Credential) StringAccessKeys() []string {
	var current, chain = credential, make([]string, 0, 4)
	for current != nil {
		chain = append(chain, string(current.AccessKey))
		current = current.next
	}
	return chain
}

func (credential *Credential) Append(next *Credential) (newCredential *Credential) {
	newCredential = credential.Clone()
	var current = newCredential
	for current.next != nil {
		current = current.next
	}
	current.next = next
	next.pred = current
	return
}

func (credential *Credential) Attach(next *Credential) {
	var current = credential
	for current.next != nil {
		current = current.next
	}
	current.next = next
	next.pred = current
}

func CredentialChainFromToken(token []byte, levelParam ...int) CredentialChain {
	var maxLevel = -1 // 0: until node
	if len(levelParam) > 0 {
		maxLevel = levelParam[0]
	}
	var credentialChain = &Credential{}
	var currentCredential = credentialChain
	for level := 0; ; level++ {
		if maxLevel >= 0 && level > maxLevel {
			break
		}
		var decoded = make([]byte, base64.StdEncoding.DecodedLen(len(token)))
		if n, err := base64.StdEncoding.Decode(decoded, token); err != nil {
			return nil
		} else if (n+2)/3 < (len(decoded)+2)/3 {
			return nil
		} else {
			var splits = bytes.SplitN(decoded[:n], []byte{0x3a}, 2)
			if len(splits) >= 2 {
				currentCredential.accessSecret = decoded[:n]
				currentCredential.next = &Credential{
					pred: currentCredential,
				}
				currentCredential = currentCredential.next
				currentCredential.AccessKey = splits[0]
				token = splits[1]
			} else {
				currentCredential.accessSecret = splits[0]
				break
			}
		}
		level++
	}
	return credentialChain.next
}

func GenerateAccessKey(lengthArg ...uint8) []byte {
	return []byte(GenerateStringAccessKey(lengthArg...))
}

func GenerateStringAccessKey(lengthArg ...uint8) string {
	var length = uint8(12)
	if len(lengthArg) > 0 && lengthArg[0] > 0 {
		length = lengthArg[0]
	}
	return random.String(length, accessKeyCharset)
}
