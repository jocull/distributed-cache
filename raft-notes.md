## Helpful links and documents:

- Official website, links, project repos, talks, more https://raft.github.io/
- Official paper: https://raft.github.io/raft.pdf
    - Page 4 includes the best summary, with some example data models and rules
- Apache Ratis, one Java implementation with some examples https://ratis.apache.org/
- SOFAJRaft, another Java implementation with some examples https://github.com/sofastack/sofa-jraft
- gRPC wiki, which some of the above implementations chose https://en.wikipedia.org/wiki/GRPC
    - In Java: https://github.com/grpc/grpc-java
- JSON-RPC wiki, which might be more compatible due to less complex implementation https://en.wikipedia.org/wiki/JSON-RPC
    - Spring tutorial on implementing WebSockets in Java https://spring.io/guides/gs/messaging-stomp-websocket/

## Notes from "Secret Lives of Data" demonstration

As linked from official website: http://thesecretlivesofdata.com/raft/

This helpful guide demonstrates lifecycles and phases in Raft so they are easy to see and understand.

### Leader elections

#### Election timeout
  - Amount of time a follower waits until becoming a candidate
  - Randomized to be between 150ms and 300ms
  - If a node hits election timeout a new election term starts

#### Election term

- Node hitting timeout votes for itself
    - Sends `RequestVote` message out to other nodes
- If node receiving hasn't voted yet in this term then it votes for the candidate and waits for next election timeout
- Candidate with majority of votes becomes leader
- Leader begins sending out `AppendEntries` messages to followers (heartbeats? logs?)
    - Messages are sent in intervals specified by the heartbeat timeout
    - Followers respond to append entries with an ack basically
    - On the next heartbeat, the followers are informed that the entry was committed, making it final on their end
- Election term will continue until a follower stops receiving heartbeats and becomes a candidate
    - If any leader node sees a higher term number appear it will step down

  ### Log replication

- Leaders need to replicate all changes to followers
    - Uses same append entries as for heartbeats
- Client sends a change to leader
    - Change is appended to leader's log
    - On next heartbeat, change is sent to followers
    - Entry is committed once a majority of followers acknowledge it
        - Which triggers a response to the client
        - If a majority cannot acknowledge, it doesn't move forward
- Any uncommitted logs are rolled back (discarded) if a node sees an election term change
