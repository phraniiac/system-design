package com.personal.sysdes.week3.deadlockpreventionalgo;

/**
 * https://www.cs.colostate.edu/~cs551/CourseNotes/Bankers.html#:~:text=Disadvantages%20of%20the%20Banker's%20Algorithm&text=It%20requires%20that%20the%20number,a%20finite%20amount%20of%20time.
 *
 * Disadvantages of the Banker's Algorithm
 * 1. It requires the number of processes to be fixed; no additional processes can start while it is executing.
 * 2. It requires that the number of resources remain fixed; no resource may go down for any reason without the possibility of deadlock occurring.
 * 3. It allows all requests to be granted in finite time, but one year is a finite amount of time.
 * 4. Similarly, all of the processes guarantee that the resources loaned to them will be repaid in a finite amount of time. While this prevents absolute starvation, some pretty hungry processes might develop.
 * 5. All processes must know and state their maximum resource need in advance.
 */
public class BankersAlgo {
}
