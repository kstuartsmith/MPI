using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace mpi {

    // This is a portion of the MPI library, implemented in C#,
    // taking advantage of facilities available in a modern OOP
    // language. There is much to be completed, but even this
    // small section of the library provides a surprising level
    // of capability.
    //
    // Release History:
    //	    11 Nov 2021: prerelease v0.1    K Stuart Smith
    //			initial functionality
    //      12 Nov 2021: prerelease v0.1.1  K Stuart Smith
    //			added local accumulator to Reduce
    //			added completion barrier to Reduce
    //
    public static class MPI {
        public static bool Debugging { get; private set; }

        // General Properties, attributes, configuration info,
        // and so on. Read the comments below for specfics.

        // IAm is the (currently) self-assigned node rank in
        // the global communication pool. Currently, it is
        // the user's responsibility to ensure that all node
        // numbers are consecutive integers, beginning at 0.

        public static long IAm { get; private set; }

        // The number of Nodes that the MPIServer believes
        // are part of the global communication pool. This is
        // reported during initialization. While this should
        // be considered a reaonable initial value, as of this
        // release, there is no maintenance of the field.

        public static long NodeCount { get; private set; }

        // The next set of private members deal with the
        // phyical message layer between this Node and the
        // MPIServer. 

        private static TcpClient client;

        private static StreamReader incoming;
        private static StreamWriter outgoing;

        private static List<Message> incomingQueue;
        private static Queue<Message> outgoingQueue;

        // Intended to be a dictionary of barriers, but most
        // likely only one is needed. Next release...

        private static Dictionary<long, ManualResetEvent> BarTheWay;

        // Out-of-band messages are used for coordination between
        // the MPIServer and the client. They are identified by a
        // negative source number (certainly not a "hack-proof"
        // scheme) and are not enqueued but handled immediately,
        // hence, "out of band."

        private static void HandleOutOfBand(Message msg) {
            switch ((ServCall)msg.Source) {
                // response "ack" from MPIServer upon Init. The
                // only thing important is the return of the
                // server's idea of the number of nodes in the
                // communication pool. This number is NOT the
                // number of connected nodes--just the server's
                // anticipation. Good enough for now.

                case ServCall.Init: {
                        NodeCount = msg.Tag;
                        break;
                    }

                // dropping a barrier. Currently, this is done
                // by the MPIServer, but needs a more local
                // mechanism.

                case ServCall.Barrier: {
                        BarTheWay[(msg.Tag)].Set();
                        break;
                    }
            }
        }

        // Hah! I told you they were useful! Monitors for handling
        // the incoming and outgoing message queues. Note that we
        // use the queues themselves as the lock key.

        private static void EnqueueOutgoing(Message msg) {
            lock (outgoingQueue) {
                outgoingQueue.Enqueue(msg);
                Monitor.PulseAll(outgoingQueue);
            }
        }

        private static Message DequeueOutgoing() {
            Message msg;
            lock (outgoingQueue) {
                while (outgoingQueue.Count == 0)
                    Monitor.Wait(outgoingQueue);
                msg = outgoingQueue.Dequeue();
            }
            return msg;
        }



        private static void EnqueueIncoming(Message msg) {
            if (msg.Source < 0)
                // these are control messages, not data.
                // process these immeidately!
                //
                HandleOutOfBand(msg);
            else
                lock (incomingQueue) {
                    incomingQueue.Add(msg);
                    Monitor.PulseAll(incomingQueue);
                }
        }

        private static Message DequeueIncoming(long source) {
            Message msg;
            int ndx = 0;
            int searchQueue() {
                for (int i = 0; i < incomingQueue.Count; i++)
                    if (incomingQueue[i].Source == source)
                        return i;
                return -1;
            }

            lock (incomingQueue) {
                while (incomingQueue.Count == 0 ||
                       (source != -1 && (ndx = searchQueue()) == -1))
                    Monitor.Wait(incomingQueue);

                msg = incomingQueue[ndx];
                incomingQueue.RemoveAt(ndx);
            }
            return msg;
        }

        // Here are (mostly) the public library methods/functions. These
        // constitute most of the API.

        ////////// MPI.Init //////////
        // This needs to be invoked before the user attempts to use any
        // other component of the MPI library--and that really ought to
        // be enforced someday.
        // There are two main functions here: establishing the physical
        // connection with the MPIServer (TCP/Ip), and startng tasks to
        // handle the incoming and outgoing message queues.
        //
        public static void Init(string host, int servicePort, long IAm,
                     bool debug = false) {
            // do everything necessary to be a good node client.

            MPI.IAm = IAm;
            Debugging = debug;
            BarTheWay = new();

            (client = new()).Connect(host, servicePort);

            incoming = new(client.GetStream());
            outgoing = new(client.GetStream()) {
                AutoFlush = true
            };

            incomingQueue = new();
            outgoingQueue = new();

            // messages are ALWAYS sent and received fully encoded and decoded
            // on receipt. That way, the user doesn't have to mess with things
            // like encoded strings (Encapsulation, my friends).
            //
            // These async tasks keep the data flowing even while the applica-
            // tion thread(s) may be blocked (for example, waiting on a barrier
            // release message).
            //
            Task.Run(async () => {
                while (true)
                    await outgoing.WriteLineAsync(DequeueOutgoing().Encode());
            });

            Task.Run(async () => {
                while (true)
                    EnqueueIncoming(Message.Decode(await incoming.ReadLineAsync()));
            });

            // The "first message"! Introduce ourselves to the MPIServer at a
            // logical level (the physical must already be established).

            SendMsg((long)ServCall.Init, "init", IAm);

            // set a barrier that must be satisfied before we finish the
            // initialization. At that point, ALL nodes will have gotten
            // to this point. That is, we wait until all of the clients
            // that the MPIServer is expecting have connected. After that
            // the MPISever will send an out-of-band message releasing
            // the barrier. (See HandleOutOfBand.)
            //
            Barrier(-79); // an unlikely prime number!

            // At this point:
            //	    This node is connected physically
            //      This node is logically connected
            //      All NodeCount nodes are logically connected
            //
            // Don't break anything!
        }

        ////////// MPI.Terminate //////////
        // This really does very little. Send a final message to the
        // server, indicating that we are going away. The MPIServer
        // can get rid of resources on it's end (queues, tasks, etc.)
        // Then we shut down the client, streams are released, etc.
        //
        public static void Terminate(string cause = "nominal") {

            SendMsg(ServCall.Terminate, cause, IAm);

            client.Close();
            incoming.Dispose();
            outgoing.Dispose();
        }

        ////////// MPI.SendMsg //////////
        // Simply, asynchronously send a message. All that needs to
        // happen is that the message needs to be enqueued and the
        // concurrent output task will take it from there.
        //
        public static void SendMsg(long toWhom,      // receiver's id (IAm)
                        object txt,       // the message
                        long tag = 0) =>  // generally not used
            EnqueueOutgoing(new(IAm, toWhom, txt, tag));

        // a convenient overload for sending Out-of-band messages
        //
        private static void SendMsg(ServCall call, object txt, long tag) =>
            SendMsg((long)call, txt, tag);

        ////////// MPI.RecvMsg //////////
        // Synchronously receive a message (blocks). Generally, the user
        // will use the related RecvText function (see below), but there
        // may be an argument for letting the user get hold of an entire
        // Message, including header information: We'll see...
        //
        public static Message RecvMsg(long source = -1) =>
            DequeueIncoming(source);

        ////////// MPI.RecvText //////////
        // This is useful: given a type, return the reconstituted message
        // text field (message body) to the specified type.  Users should
        // typically use this interface as opposed to RecvMsg. Once again:
        // the user is responsible for being sure the casting makes sense.
        //
        public static T RecvText<T>(long source = -1) =>
            DequeueIncoming(source).ToType<T>();

        ////////// MPI.Barrier //////////
        // Establish and wait on a Barrier. This is a blocking call and
        // the caller will wait until the Barrier is cleared by the MPI-
        // Server when all nodes have reached the Barrier. The tag must
        // be a positive integer (again, should be enforced) that all of
        // the synchronizing Nodes agree upon.
        //
        public static void Barrier(long tag) {
            ManualResetEvent barrier = new(false);
            BarTheWay.Add(tag, barrier);

            SendMsg(ServCall.Barrier, 0, tag);
            barrier.WaitOne();

            BarTheWay.Remove(tag);
        }

        ////////// MPI.Print //////////
        // A service function that causes the ToString of the object to
        // be sent to the MPIServer where it will be displayed on the
        // server's console.
        //
        public static void Print(object text) =>
            SendMsg(ServCall.Print, text.ToString(), 0);

        ////////// MPI.Broadcast //////////
        // The message is sent to all Nodes in the communication pool
        // (from the MPIServer--this really needs to be reworked). The
        // message is typically not repeated to the originating Node
        // but that option can be overridden.
        //
        public static void Broadcast(object text, bool excludeOriginator = true) =>
            SendMsg(ServCall.Broadcast, text, excludeOriginator ? 1 : 0);

        ////////// MPI.Reduce //////////
        // From the perspective of the user, MPI.Reduce performs a
        // reduction of per-node data items using global communication fold.
        // The delegate ReduceFunc defines a type for a function that performs
        // an associative binary opertion. (Examples are Sum, Product, Xor,
        // Min, Max, etc.)
        //
        // Explanation for "why" it works the way that it does is not
        // not something easily handled here. See external documentation.
        //
        public delegate T ReduceFunc<T>(T a, T b);

        // as implemented, Reduce only works properly if there are exactly
        // 2**N nodes. The change is small, but it is not critical. I will
        // try to get an update in the next few days.

        public static void Reduce<T>(long at, ref T contrib, ReduceFunc<T> f) {
            T accumulator = contrib; // no good reason to be destructive...

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
                var low = IAm & ~mask;
                var high = IAm | mask;
                var iamLow = IAm == low;
                var fold2high = (at & mask) != 0;

                // This very obscure algorithm is a reduction of a much more
                // transparent algorithm, but that required far more operations
                // and tests to be performed. What is left is the kernel.

                if (fold2high != !iamLow) {
                    // DoSend (iamlow ? high : low);
                    if (Debugging)
                        Console.WriteLine("{0} send to {1}",
                                   MPI.IAm, iamLow ? high : low);
                    MPI.SendMsg(iamLow ? high : low, accumulator);
                    break;
                } else {
                    // DoRecv (iamlow ? high : low);
                    if (Debugging)
                        Console.WriteLine("{0} recv fr {1}",
                                   MPI.IAm, iamLow ? high : low);
                    T recved = MPI.RecvText<T>();
                    if (Debugging)
                        Console.WriteLine("{0} received {1}", MPI.IAm, recved);
                    accumulator = f(recved, accumulator);
                    if (MPI.IAm == at)
                        // copy back...
                        contrib = accumulator;
                }

                // on to the next dimension. nodes that "sent" at the former
                // fold are no longer participating.

                addrSize--;
                mask <<= 1;
            }

            // if a "finished" node goes ahead, it could really mess up
            // the rest of the pattern by injecting a send that gets
            // interpreted as being part of the reduction.

            MPI.Barrier(83);
        }
    }
}
