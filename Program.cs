using System;
using System.Collections.Generic;

namespace mpi {
    class MPIProgram {
        private static bool Debugging { get; set; }

        // a couple of methods to use with MPI.Reduce. These match
        // the delegate MPI.ReductFunc.
        //
        //	public delegate T ReduceFunc<T>(T a, T b);

        public static long Product(long a, long b) => a * b;
        public static long Summation(long a, long b) => a + b;

        public static void Main(string[] args) {
            Debugging = (args.Length > 3 && args[3] == "-debug");

            // establish connection and wait for everyone else
            MPI.Init(args[0],           // MPIServer host
                 Int32.Parse(args[1]),  // MPIServer port
                 Int32.Parse(args[2]),  // This node number
                 Debugging);

            // A test of Send/Receive. Node 0 is generally assumed
            // the "master" node of the application simply because
            // there will ALWAYS be a Node 0 at the start.
            //
            // Node 0 will send a List<int> to Node 1 who will try
            // to receive the List and print its contents. Note we
            // have to mess around with buffers, etc.: all of that
            // is managed behinds the scenes.


            if (MPI.IAm == 0 && MPI.NodeCount > 1) {
                // MPI.Print sends a string to the MPIServer console.
                // It will be prefixed with [IAm] when printed.
                MPI.Print("sending...");

                // Create a list like you would in any environment:
                // this is just a C# program, after all.		
                List<int> myList = new() { 1, 2, 3, 4 };

                // Not a whole lot to this: what Node is the message
                // going to and what is the text.
                MPI.SendMsg(1, myList);
            } else if (MPI.IAm == 1) {
                // Since this is Node 1, it can be assumed that the
                // master node has sent a message (above).
                MPI.Print("receiving...");

                // We should expect a List<int>. It is important to
                // understand that the application must be written
                // in such a way to keep everything synced up both
                // with regard to sends/receives and with types.

                var tobj = MPI.RecvText<List<int>>(0);

                // just to prove we did the write right. Look at the
                // MPIServer's console for the report.
                foreach (var i in tobj)
                    MPI.Print(i);
            }

            // Demonstrate a reduction. Each node will contribute
            // a data item (reduceThis) and the contributions will
            // be "summed" and left at one node's doorstep. For
            // this demonstration, the "last node" will be used as
            // the collector.

            if (MPI.NodeCount > 1) {
                // define a value to be collected. Note that this data
                // will be overwritten at the destination node!

                long reduceThis = MPI.IAm + 1;

                // reduction of reduceThis via function Product with the
                // final reduced value ending up at the "last" node. 
                // MPI.Reduce<long>(MPI.NodeCount - 1, ref reduceThis, Product);

                // And, if we happen to be the colletor, we will show 
                // that the reduction completed successfully. The result
                // should be factorial(NodeCount).

                // if (MPI.IAm == MPI.NodeCount - 1)
                //     MPI.Print("reduction: " + reduceThis);

                // long reduceAllThis = MPI.IAm + 1;
                // MPI.ReduceAll<long>(ref reduceAllThis, Product);
                // MPI.Print("Reduce all: " + reduceAllThis);

                // var myGatherList = new List<long> { MPI.IAm, -MPI.IAm };
                // var gatherResult = MPI.Gather<long>(7, myGatherList);

                // if (MPI.IAm == 7) {
                //     MPI.Print("Gather result: [" + string.Join(",", gatherResult) + "]");
                // }

                // var myGatherList2 = new List<long> { MPI.IAm, -(MPI.IAm) };
                // var gatherResult2 = MPI.Gather<long>(7, myGatherList2);

                // if (MPI.IAm == 7) {
                //     MPI.Print("Gather result: [" + string.Join(",", gatherResult2) + "]");
                // }

                // var myGatherList3 = new List<long> { MPI.IAm, -(MPI.IAm) };
                // var gatherResult3 = MPI.Gather<long>(5, myGatherList3);

                // if (MPI.IAm == 5) {
                //     MPI.Print("Gather result: [" + string.Join(",", gatherResult3) + "]");
                // }

                // var myGatherList4 = new List<long> { MPI.IAm, -(MPI.IAm) };
                // var gatherResult4 = MPI.Gather<long>(3, myGatherList4);

                // if (MPI.IAm == 3) {
                //     MPI.Print("Gather result: [" + string.Join(",", gatherResult4) + "]");
                // }
            }

            if (MPI.IAm == 7) {
                // If I am the 0 node...
                var testBroadcast = 42;
                MPI.Print("Broadcasting from 7: " + testBroadcast);
                CMPI.BroadcastJP<int>(ref testBroadcast, 7);
            } else {
                var bCastResult = -1;
                CMPI.BroadcastJP<int>(ref bCastResult, 7);
                MPI.Print("Broadcast result: " + bCastResult);
            }


            if (MPI.IAm == 7) {
                // If I am the 7 node...
                var testBroadcast = 47;
                MPI.Print("Broadcasting from 7: " + testBroadcast);
                CMPI.BroadcastJP<int>(ref testBroadcast, 7);
            } else {
                var bCastResult = -1;
                CMPI.BroadcastJP<int>(ref bCastResult, 7);
                MPI.Print("Broadcast result: " + bCastResult);
            }

            var myGatherList = new List<long> { MPI.IAm, -MPI.IAm };
            var gatherResult = CMPI.Gather<long>(7, myGatherList);

            if (MPI.IAm == 7) {
                MPI.Print("Gather result: [" + string.Join(",", gatherResult) + "]");
            }

            MPI.Barrier(13);

            var myGatherList2 = new List<long> { MPI.IAm, -(MPI.IAm) };
            var gatherResult2 = CMPI.Gather<long>(7, myGatherList2);

            if (MPI.IAm == 7) {
                MPI.Print("Gather result: [" + string.Join(",", gatherResult2) + "]");
            }

            MPI.Barrier(14);

            var myGatherList3 = new List<long> { MPI.IAm, -(MPI.IAm) };
            var gatherResult3 = CMPI.Gather<long>(7, myGatherList3);

            if (MPI.IAm == 7) {
                MPI.Print("Gather result: [" + string.Join(",", gatherResult3) + "]");
            }

            MPI.Barrier(15);

            var myGatherList4 = new List<long> { MPI.IAm, -(MPI.IAm) };
            var gatherResult4 = MPI.Gather<long>(3, myGatherList4);

            if (MPI.IAm == 3) {
                MPI.Print("Gather result: [" + string.Join(",", gatherResult4) + "]");
            }
            var gatherAllResult = MPI.GatherAll<long>(myGatherList);
            MPI.Barrier(1021);
            MPI.Print("Gather all result length: " + gatherAllResult.Count);
            if (MPI.IAm == 3) {
                foreach (var i in gatherAllResult) {
                    MPI.Print("GATHER ALL RESULT PART: " + i);
                }
            }

            // Finally, wait for everyone to get here. The tag 30 is
            // not magical--can be any positive integer that all of
            // the nodes agree to.

            MPI.Barrier(30);

            // bye, bye.
            MPI.Terminate();
        }
    }
}
