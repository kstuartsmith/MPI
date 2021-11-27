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

	public static void Main (string[] args) {
	    Debugging = (args.Length > 3 && args[3] == "-debug");

	    // establish connection and wait for everyone else
	    MPI.Init(args[0],			// MPIServer host
		     Int32.Parse(args[1]),	// MPIServer port
		     Int32.Parse(args[2]),	// This node number
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
		List<int> myList = new(){1, 2, 3, 4};

		// Not a whole lot to this: what Node is the message
		// going to and what is the text.
		MPI.SendMsg (1, myList);
	    }
	    else if (MPI.IAm == 1) {
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
		MPI.Reduce<long>(MPI.NodeCount-1, ref reduceThis, Product);

		// And, if we happen to be the colletor, we will show 
		// that the reduction completed successfully. The result
		// should be factorial(NodeCount).
		
		if (MPI.IAm == MPI.NodeCount-1)
		    MPI.Print("reduction: " + reduceThis);
	    }

	    // finally, demonstrate broadcast...

	    if (MPI.NodeCount > 1) {
		long bData = 0;
		if (MPI.IAm == MPI.NodeCount-1)
		    bData = MPI.IAm;
	    
		MPI.Broadcast<long>(MPI.NodeCount-1, ref bData); 
		MPI.Print("bcast: " + bData);
	    }

	    // wait for everyone to get here--technically not
	    // necessary since Reduce and Broacast both sync.
	    
	    MPI.Barrier(30);

	    // bye, bye.
	    MPI.Terminate();
	}
    }
}
