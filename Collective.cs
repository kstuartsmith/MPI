using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace mpi {

    static class CMPI {
        public static int findBroadcastLevel(long root) {
            var addrSize = BitOperations.Log2((ulong)MPI.NodeCount);

            for (int i = addrSize - 1; i >= 0; i--) {
                var mask = 1 << i;
                var result = (root ^ MPI.IAm) & mask;
                if (result != 0) {
                    return i;
                }
            }
            return 0; // This is the source, as there are no different digits.
        }

        public static void BroadcastJP<T>(ref T text, long root) {
            var addrSize = BitOperations.Log2((ulong)MPI.NodeCount);
            long mask = 1;
            long lastDimMask = 1 << addrSize - 1;
            Boolean isSource = root == MPI.IAm;
            Boolean msgReceived = false;

            var bCastLevel = findBroadcastLevel(root);
            // These two lines are necessary to "calibrate" where in the process
            mask <<= bCastLevel;
            addrSize -= bCastLevel;

            while (addrSize > 0) {
                var low = MPI.IAm & ~mask;
                var high = MPI.IAm | mask;
                var iamLow = MPI.IAm == low;

                // This very obscure algorithm is a reduction of a much more
                // transparent algorithm, but that required far more operations
                // and tests to be performed. What is left is the kernel.

                if (isSource || msgReceived) {
                    if (MPI.Debugging)
                        Console.WriteLine("{0} send to {1}",
                                   MPI.IAm, iamLow ? high : low);
                    MPI.SendMsg(iamLow ? high : low, text);
                } else {
                    var receivingFrom = iamLow ? high : low;
                    if (MPI.Debugging)
                        Console.WriteLine("{0} recv fr {1}",
                                   MPI.IAm, receivingFrom);
                    text = MPI.RecvText<T>();
                    msgReceived = true; // We have received the message, so switch to send mode.
                    if (MPI.Debugging)
                        Console.WriteLine("{0} received {1}", MPI.IAm, text);
                }

                // on to the next dimension.

                addrSize--;
                mask <<= 1;
            }

            MPI.Barrier(73);
        }

        public static void ReduceAll<T>(ref T contrib, MPI.ReduceFunc<T> f) {
            long at = MPI.NodeCount - 1; // Arbitrarily reduce to last node.
            MPI.Reduce<T>(at, ref contrib, f);
            MPI.Barrier(2);
            MPI.BroadcastJP<T>(ref contrib, at);


            MPI.Barrier(47);
        }

        public static List<T> Gather<T>(long toWhere, List<T> toBeGathered) {
            var accumulator = toBeGathered; // no good reason to be destructive...

            var addrSize = BitOperations.Log2((ulong)MPI.NodeCount);
            long mask = 1;     // fold dimension order: 1, 2, 3 ... addrSize

            // addrSize corresponds to the number of foldable dimentions.
            // mask "walks" those dimensions from the lowest to highest. Using
            // this pattern, one can use the destination (at) node number to
            // determine how the fold occurs. When a dimension is folded, the
            // nodes involved in the fold are distinquished by one half
            // having "lower" node numbers than those of corresponding nodes in
            // the other other half. These node pairs send data either "high"
            // to "low" or "low" to "high" depending on the value of the mask-
            // selected bit of the destination node.

            // It should probably be noted that once a node "sends" it's data
            // to another node, that represents the end of its participation
            // in the communcation. A node may receive data from several nodes
            // but only sends once. That means that the destinatio node only
            // receives, and receives the last message sent.

            while (addrSize > 0) {
                var low = MPI.IAm & ~mask;
                var high = MPI.IAm | mask;
                var iamLow = MPI.IAm == low;
                var fold2high = (toWhere & mask) != 0;

                // This very obscure algorithm is a reduction of a much more
                // transparent algorithm, but that required far more operations
                // and tests to be performed. What is left is the kernel.

                if (fold2high != !iamLow) {
                    if (MPI.Debugging)
                        Console.WriteLine("{0} send to {1}",
                                   MPI.IAm, iamLow ? high : low);
                    MPI.SendMsg(iamLow ? high : low, accumulator);
                    break;
                } else {
                    var receivingFrom = iamLow ? high : low;
                    if (MPI.Debugging)
                        Console.WriteLine("{0} recv fr {1}",
                                   MPI.IAm, receivingFrom);
                    List<T> recved = MPI.RecvText<List<T>>(receivingFrom);
                    if (MPI.Debugging)
                        Console.WriteLine("{0} received {1}", MPI.IAm, recved);

                    // This should ensure that info gets added in the correct order...
                    if (iamLow) {
                        accumulator.AddRange(recved);
                    } else {
                        recved.AddRange(accumulator);
                        accumulator = recved;
                    }

                    if (MPI.IAm == toWhere)
                        // copy back...
                        toBeGathered = accumulator;
                }

                // on to the next dimension. nodes that "sent" at the former
                // fold are no longer participating.

                addrSize--;
                mask <<= 1;
            }

            // if a "finished" node goes ahead, it could really mess up
            // the rest of the pattern by injecting a send that gets
            // interpreted as being part of the reduction.

            MPI.Barrier(834);
            return toBeGathered;
        }

        public static List<T> GatherAll<T>(List<T> toBeGathered) {
            var gatherResult = MPI.Gather<T>(0, toBeGathered);
            MPI.Barrier(3);

            MPI.BroadcastJP<List<T>>(ref gatherResult, 0);
            MPI.Barrier(6);
            return gatherResult;
        }
    }
}
