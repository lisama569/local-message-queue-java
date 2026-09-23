package com.github.lisama569.localmessagequeuejava.mq.store;

import com.github.lisama569.localmessagequeuejava.mq.model.PartitionState;
import com.github.lisama569.localmessagequeuejava.mq.model.TopicPartition;

import java.util.Map;

/**
 * 消费者组分区位点与未确认队头状态存储，重启后可恢复。
 */
public interface GroupStateStore {

    Map<TopicPartition, PartitionState> loadAll();

    PartitionState load(TopicPartition tp);

    /**
     * 原子保存整个组状态（临时文件 + 原子 move + fsync）。
     */
    void saveAll(Map<TopicPartition, PartitionState> states);
}
