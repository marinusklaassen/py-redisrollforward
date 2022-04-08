Feature: The redis_aof_reconstruct and redis_aof_recreate servers 
  In order to collect aofchunks from the aofbackup directory and recreate a redis database at a specific time
  As a developer
  I want to implement a system to recreate a redis database state at a specific time

  Background: a series of archived aofchunks with the same bgrewriteaof datetime measured over several days
    Given a backup directory "aofbackupdir" for server address "127.0.0.1"   
    And a series of archived aofchunks located in several subfolders 1
     | Directory  | Filename                                                                |
     | 2015-03-30 | dwan_6379__2015-03-30_02-00-00_-_2015-03-30_02-00-00__000001_000001.aof | 
     | 2015-03-31 | dwan_6379__2015-03-30_02-00-00_-_2015-03-31_02-00-00__000001_000001.aof |
     | 2015-04-01 | dwan_6379__2015-03-30_02-00-00_-_2015-04-01_02-00-00__000001_000001.aof |
    And dwan_6379__2015-03-30_02-00-00_-_2015-03-30_02-00-00__000001_000001.aof contains 
      """
      *2
      $6
      SELECT
      $1
      0
      *3
      $3
      SET
      $9
      schaapjes
      $9
      asscjaphe
      *3
      $3
      SET
      $7
      miesjes
      $6
      jpasea

      """
    And dwan_6379__2015-03-30_02-00-00_-_2015-03-31_02-00-00__000001_000001.aof contains
      """
      *3
      $3
      SET
      $7
      nootjes
      $6
      peasja

      """
    And dwan_6379__2015-03-30_02-00-00_-_2015-04-01_02-00-00__000001_000001.aof contains
      """
      *3
      $3
      SET
      $7
      miesjes
      $7
      msjseei

      """
      
  Scenario: Recreating a redis database state
    Given there is a running redis_aof_reconstruct server 
    And   there is a running redis_aof_recreate server
    When  I request a redis database state with following the json instruction via a socket
      """
      {
        "source_redistag"             : "dwan",
        "source_redisport"            : "6379", 
        "source_redisserver"          : "127.0.0.1",
        "atdatetime"                  : "2015-04-01 04:44:44",
        "target_redis_server_summary" : "test.tst.dwan__6379"
      }
      """
    Then I see a redis instance on localhost and port 45321 with the following key values
        | key       | value     |
        | schaapjes | asscjaphe |
        | miesjes   | msjseei   |
        | nootjes   | peasja    |
    
