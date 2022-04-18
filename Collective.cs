using System;
using System.Collections.Generic;
using System.Numerics;

namespace mpi {
    public static class Collective {
	public static void ReduceAll<T>(ref T toBeReduced, MPI.ReduceFunc<T> f) {
	    MPI.Reduce<T>(0, ref toBeReduced, f);
	    MPI.Broadcast<T>(0, ref toBeReduced);
	}

	public static List<T> GatherAll<T>(List<T> toGather) {
	    List<T> gathered = Gather<T>(0, toGather);
	    MPI.Broadcast<List<T>>(0, ref gathered);
	    return gathered;
	}

	public static List<T> Gather<T>(long at, List<T> toGather) {
	    List<T> gathered = new();
	    gathered.AddRange(toGather);

	    var addrSize = BitOperations.Log2((ulong)MPI.NodeCount);
	    long mask = 1;     // fold dimension order: 1, 2, 3 ... addrSize
	    
	    while (addrSize > 0) {
		var low = MPI.IAm & ~mask;
		var high = MPI.IAm | mask;
		var iamLow = MPI.IAm == low;
		var fold2high = (at & mask) != 0;
		
		// This very obscure algorithm is a reduction of a much more
		// transparent algorithm, but that required far more operations
		// and tests to be performed. What is left is the kernel.

		if (fold2high == iamLow) {
		    // DoSend (iamlow ? high : low);
		    if (MPI.Debugging)
			Console.WriteLine ("G {0} send to {1}",
					   MPI.IAm, iamLow ? high : low);
		    MPI.SendMsg (iamLow ? high : low, gathered);
		    break;
		}
		else { 
		    // DoRecv (iamlow ? high : low);
		    if (MPI.Debugging)
			Console.WriteLine ("G {0} recv fr {1}",
					   MPI.IAm, iamLow ? high : low);
		    var recved = MPI.RecvText<List<T>>(iamLow ? high : low);
		    if (iamLow)
			gathered.AddRange (recved);
		    else
			gathered.InsertRange (0, recved);
		}
		
		// on to the next dimension. nodes that "sent" at the former
		// fold are no longer participating.

		addrSize--;
		mask <<= 1;
	    }

	    // if a "finished" node goes ahead, it could really mess up
	    // the rest of the pattern by injecting a send that gets
	    // interpreted as being part of the reduction.
	    
	    MPI.Barrier(-23);
	    return (MPI.IAm == at ? gathered : null);
	}

	public static List<T> Scatter<T>(long source, List<T> toScatter = null) {
	    var addrSize = BitOperations.Log2((ulong)MPI.NodeCount);
	    long mask = 1 << (addrSize-1); // fold order: addrSize ... 3, 2, 1

	    // No longer will Scatter trash the toScatter list!
	    
	    toScatter = ((toScatter == null) ? new() : new(toScatter)); 

	    while (addrSize > 0) {
		var low = MPI.IAm & ~mask;
		var high = MPI.IAm | mask;
		var iAmLow = MPI.IAm == low;
		var fold2High = (source & mask) == 0;

		// this "clever" little expression determines whether a
		// node is "participating" in this fold. It essentially
		// compares addrSize-1 lowest bits of IAm with those of
		// the source. If they match (xor is 0), this node will
		// participate.
		//
		var participating = ((mask - 1) & (MPI.IAm ^ source)) == 0;

		// this is the same mysterious logic as used in Reduce.
		// Go there for some weak explanations.
		
		if (participating) {
		    if (fold2High == iAmLow) {
			int split = toScatter.Count/2;
			int scatSize = iAmLow ? toScatter.Count - split : split;
			List<T> scatter
			    = toScatter.GetRange(iAmLow ? split : 0, scatSize);
			toScatter.RemoveRange(iAmLow ? split : 0, scatSize);
			if (MPI.Debugging)
			    Console.WriteLine ("S {0} send to {1}",
					       MPI.IAm, iAmLow ? high : low);
			MPI.SendMsg(iAmLow ? high : low, scatter);
		    }
		    else {
			if (MPI.Debugging)
			    Console.WriteLine ("S {0} recv fr {1}",
					       MPI.IAm, iAmLow ? high : low);
			var scattered = MPI.RecvText<List<T>>(iAmLow ? high : low);
			toScatter.AddRange(scattered);
		    }
		}
		
		addrSize--;
		mask >>= 1;
	    }
	    MPI.Barrier (-67);
	    return toScatter;
	}
    }
}
