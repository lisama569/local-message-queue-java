package com.github.lisama569.localmessagequeuejava.mq.handler;

import com.github.lisama569.localmessagequeuejava.mq.model.Message;

/**
 * 消息处理器。业务副作用应与消息 id 幂等绑定；
 * 抛出 NonRetryableException 立即死信，其它异常按可重试处理。
 */
public interface MessageHandler {
    void handle(Message message) throws Exception;
}
