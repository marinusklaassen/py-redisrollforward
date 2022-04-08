Feature: The aof archive collector
  In order to collect aofchunks from the aofbackup directory
  As a developer
  I want to implement a method that collects aofchunks from the aofbackup directory

  Background: a series of archived aofchunks with the same bgrewriteaof datetime measured over several days
    Given a server backup directory "aofbackupdir" for server address "127.0.0.1"   
    And a series of archived aofchunks located in several subfolders
     | Directory  | Filename  |
     | 2015-03-30 | dwan_6379__2015-03-30_02-00-00_-_2015-03-30_02-00-00__000001_000001.aof | 
     | 2015-03-31 | dwan_6379__2015-03-30_02-00-00_-_2015-03-31_02-00-00__000001_000001.aof |
     | 2015-04-01 | dwan_6379__2015-03-30_02-00-00_-_2015-04-01_02-00-00__000001_000001.aof |
      
  Scenario: Collecting aofchunks from the aofbackup directory
    Given an instance of the c_aof_archive_collector
    When  I request the archived aofchunks with the json instruction
      """
      {
        "redistag"   : "dwan",
        "redisport"  : "6379", 
        "server"     : "127.0.0.1",
        "atdatetime" : "2015-04-01_04-44-44"
      }
      """
    Then I see 3 collected files 
