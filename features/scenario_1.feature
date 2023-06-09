Feature: 5 cluster nodes

  Scenario: scenario 1
     Given 5 nodes, 4 Follower and a Leader
      When the Leader goes offline
      Then the Followers should elect a new Leader