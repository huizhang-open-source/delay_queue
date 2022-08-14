package delay_queue

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
)

const (
	GetMessageAckScript = `
		local messages = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, KEYS[3]);
		if #messages > 0 then
			redis.call('ZREM', KEYS[1], unpack(messages));
			local zaddArr = {}
			for index,message in ipairs(messages)
			do
				table.insert(zaddArr, ARGV[2]);
				table.insert(zaddArr, message);
			end
			redis.call('ZADD', KEYS[2], unpack(zaddArr));
			return messages;
		else 
			return {};
		end
	`
	GetMessageScript = `
		local messages = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, KEYS[3]);
		if #messages > 0 then
			redis.call('ZREM', KEYS[1], unpack(messages));
			return messages;
		else 
			return {};
		end
	`
	AckMonitorScript = `
		local messages = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, KEYS[3]);
		if #messages > 0 then
			redis.call('ZREM', KEYS[1], unpack(messages));
			local zaddArr = {}
			for index,message in ipairs(messages)
			do
				table.insert(zaddArr, 1);
				table.insert(zaddArr, message);
			end
			redis.call('ZADD', KEYS[2], unpack(zaddArr));
		end
		return {};
	`
	DelMessageScript = `
		redis.call('ZREM', KEYS[1], ARGV[1]);
		redis.call('ZREM', KEYS[2], ARGV[1]);
		return true;
	`
	NoScript = "NOSCRIPT No matching script. Please use EVAL."
)

var GetMessageLuaScript = map[string]string{
	AckTypeAuto:    GetMessageAckScript,
	AckTypeManual:  GetMessageAckScript,
	AckTypeDisable: GetMessageScript,
}

func execLuaScript(
	ctx context.Context,
	redis Redis,
	sha1 string,
	script string,
	values []interface{},
) (interface{}, error) {
	res, err := redis.EvalSha(ctx, sha1, values)
	if err == nil || err.Error() != NoScript {
		return res, nil
	}

	err = redis.LoadScript(ctx, script)
	if err != nil {
		return nil, err
	}

	res, err = redis.EvalSha(ctx, sha1, values)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func sha1Script(script string) string {
	o := sha1.New()
	o.Write([]byte(script))
	return hex.EncodeToString(o.Sum(nil))
}
