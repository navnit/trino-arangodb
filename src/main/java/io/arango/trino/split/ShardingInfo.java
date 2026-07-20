package io.arango.trino.split;

public record ShardingInfo(Integer numberOfShards, String shardingStrategy, String smartJoinAttribute) {}
