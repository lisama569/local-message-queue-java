package com.github.lisama569.localmessagequeuejava.mq.handler;

import com.github.lisama569.localmessagequeuejava.mq.model.HandleResult;
import com.github.lisama569.localmessagequeuejava.mq.model.Message;

/** 业务消息处理器。抛异常默认按可重试失败处理。 */
@FunctionalInterface
public interface MessageHandler {
    HandleResult handle(Message message) throws Exception;
}
