> ⚠️ Work In Progress
# ResonateDB
A leaderless distributed SQLite database inspired by the Dynamo Paper and Designing Data Intensive Applications. 
# Architecture
- SWIM Protocol: Membership and failiure detection
- gRPC: Node communication
- SQLite: Local Store
- Leaderless Replication: TBD
# References
- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership
Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
- [Designing Data Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)
- [Dynamo: Amazon’s Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
 # Built With
 - Go
 - gRPC
 - Protocol Buffers
 - Task File