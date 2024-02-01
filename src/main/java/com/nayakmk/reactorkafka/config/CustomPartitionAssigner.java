package com.nayakmk.reactorkafka.config;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class CustomPartitionAssigner implements ConsumerPartitionAssignor {

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return ConsumerPartitionAssignor.super.subscriptionUserData(topics);
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        Map<String, Assignment> assignments = new HashMap<>();
        Set<TopicPartition> assignedPartitions = new HashSet<>();

        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            String memberId = subscriptionEntry.getKey();
            List<String> subscribedTopics = subscriptionEntry.getValue().topics();
            List<TopicPartition> partitions = new ArrayList<>();

            for (String topic : subscribedTopics) {
                List<PartitionInfo> partitionInfos = metadata.partitionsForTopic(topic);
                for (PartitionInfo partitionInfo : partitionInfos) {
                    TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    // Check if the partition has already been assigned
                    if (!assignedPartitions.contains(topicPartition)) {
                        partitions.add(topicPartition);
                        // Mark the partition as assigned
                        assignedPartitions.add(topicPartition);
                        break; // Remove this line if you want to assign more than one partition from the same topic
                    }
                }
            }

            if (!partitions.isEmpty()) {
                assignments.put(memberId, new Assignment(partitions));
            } else {
                assignments.put(memberId, new Assignment(Collections.emptyList()));
            }
        }

        return new GroupAssignment(assignments);
    }

    @Override
    public String name() {
        return "CustomPartitionAssigner";
    }
    
}