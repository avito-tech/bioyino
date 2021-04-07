# I'm sending metrics but do not receive all of the at backend
Most probably you are working in multimessage mode which, for the sake of performance, uses buffers and OS capabilities 
heavily. That means your metrics are there, but have been stuck in buffers. What can you try to work this around:

0. send a single metric after timeout period, specified by `buffer-flush-time` setting in `[network]` section
0. decrease value in `buffer-flush-length` setting in `[network]` section
0. turn off multimessage mode if you don't have so much metrics or just testing

# I have N nodes in consensus and I want to upgrade/reboot one. How to avoid losing metrics?
First of all check if node is a leader. If it's not, all you need to do is remove metric traffic from that node.
If the node is leader, you need to migrate leader away from it **after** you've done with the traffic. With internal 
Raft this is probably not necessary, because switching leader there is very fast - less than a second. If this not a case,
you'll need to do it manually.  One of the best ways is managing the leader manually for the time of updating. To do that:

0. Pause consensus on all nodes in the cluster disabling leader on all nodes except one. This "one" must *not* be the one
you are going to turn off.
0. Now you have another leader and the node you want to upgrade can be turned off for maintenance. 
0. When turning on the node back, make sure consensus is paused and the node is not a leader.
0. You can do the same with all other nodes except the active one.
0. Before switching off the leader on the last node, make sure the node you want to be next leader has worked for 
aggregation period or more and has all the metrics for the last period
0. Switch leader to another node (there still can be a peak on graphs, because time intervals are not in sync, we hope 
this will be fixed in next versions)

