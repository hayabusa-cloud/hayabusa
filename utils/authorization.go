package utils

import "fmt"

type Authorization struct {
	*Credential
	permission [16]int32 // bit0 not in-use
	top        int
}

func NewAuthorization(cred *Credential) *Authorization {
	return &Authorization{
		Credential: cred,
		permission: [16]int32{},
		top:        0,
	}
}

func (authorization *Authorization) CheckPermission(perm int32) bool {
	for i := 0; i < 16 && i < authorization.top; i++ {
		if authorization.permission[i]&perm == perm {
			return true
		}
	}
	return false
}

func (authorization *Authorization) IsAuthorized() bool {
	for i := 0; i < 16 && i < authorization.top; i++ {
		if authorization.permission[i] > 0 {
			return true
		}
	}
	return false
}

func (authorization *Authorization) PushPermission(perm int32) (err error) {
	if authorization.top >= 16 {
		return fmt.Errorf("push permission: stack full")
	}
	authorization.permission[authorization.top] = perm
	authorization.top++
	return nil
}

func (authorization *Authorization) PopPermission() (perm int32, err error) {
	if authorization.top < 1 {
		return 0, fmt.Errorf("pop permission: empty stack")
	}
	perm, err = authorization.permission[authorization.top], nil
	authorization.top--
	return
}
