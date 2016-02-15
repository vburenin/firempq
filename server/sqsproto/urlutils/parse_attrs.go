package urlutils

import (
	"firempq/server/sqsproto/sqserr"
	"strconv"
	"strings"
)

type ParamFunc func(param string, value string) *sqserr.SQSError

type NewSubContainer func() ISubContainer

type ISubContainer interface {
	Parse(param string, value string) *sqserr.SQSError
}

func ParseNNotationAttr(prefix string, paramList []string, mainHandler ParamFunc, sc NewSubContainer) (map[int]ISubContainer, *sqserr.SQSError) {
	res := make(map[int]ISubContainer)
	pos := 0
	paramLen := len(paramList) - 1
	for ; pos < paramLen; pos += 2 {
		attrName := paramList[pos]
		if strings.HasPrefix(attrName, prefix) {
			blocks := strings.SplitN(attrName, ".", 3)
			if len(blocks) != 3 {
				return nil, sqserr.MalformedInputError("key: " + attrName + " is malformed")
			}
			v, err := strconv.Atoi(blocks[1])
			if err == nil {
				c, ok := res[v]
				if !ok {
					c = sc()
					res[v] = c
				}
				if err := c.Parse(blocks[2], paramList[pos+1]); err != nil {
					return nil, err
				}
			}
		} else if mainHandler != nil {
			mainHandler(attrName, paramList[pos+1])
		}
	}
	return res, nil
}
