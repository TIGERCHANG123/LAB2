import beans.RestResult;

import java.util.List;

public interface RaftRMI {
    RestResult appendEntries(int term, int leaderID, int prevLogIndex, int prevLogTerm, List<String> nxtLogs, int leaderCommit);
    RestResult requestVote(int term, int candidatID, int lastLogIndex, int lastLogTerm);
}
