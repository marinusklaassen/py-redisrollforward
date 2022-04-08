Feature: Follow and backup redis database changes by following the appendonly.aof file change 
  In order to restore a database statement at a specific time
  As a developer
  I want to implement a system to backup the appendonly.aof file change at a specified interval

  Scenario: Archiving the appendonly file change
    Given there is a redis instance with AOF persistence that logs every write operation received by the server
    And   there is a running redis_aof_read client 
    And   there is a running redis_aof_funnel server
    When  I make a change in the Redis database
    And   repeat 'I make a change in the Redis database' 2 times while waiting 1 second each turn
    Then  I see 3 archived files in the backup folder
