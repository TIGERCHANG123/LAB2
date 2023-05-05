
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import beans.RaftLog;
import beans.RaftRole;
import beans.RestResult;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftNode extends UnicastRemoteObject implements RaftRMI {
    //
    private ExecutorService es = Executors.newFixedThreadPool(8);

    private RaftRole role = RaftRole.FOLLOWER;
    private int msPassed = 0; // 计时
    private int followerExpire = 150; // follower -> candidate 等待时间
    private int candidatExpire = 150; // candidate -> new candidate
    private final int leaderExpire = 10; // 心跳信号的间隔

    // Raft 持久状态
    private int currentTerm = 1;
    private int voteFor = 0;
    private List<String> logs;
    // Raft 暂态
    private int leaderID = 0; // 当前 leader 的ID
    private int serverID = 0; // 当前服务器的 ID
    private int commitIndex;
    private int lastApplied;
    // Raft leader state
    private int[] nextIndex = new int[5];
    private int[] matchIndex = new int[5];

    public RaftNode(int id) throws RemoteException {
        serverID = id;
        Random rand = new Random();
        followerExpire = rand.nextInt(150) + 150; // 150 ms - 300 ms
        candidatExpire = rand.nextInt(50) + 50; // 50 ms - 100 ms
        es.submit(()->{ // 计时器
            try {
                Thread.sleep(1000); // 休眠 1ms 调试时设置为1s
                msPassed ++;
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
            switch (role) {
                case FOLLOWER -> { // follower 超时，开启选举操作
                    if (msPassed >= followerExpire) {
                        role = RaftRole.CANDIDATE;
                        msPassed = candidatExpire;
                    }
                }
                case CANDIDATE -> { // candidate 超时 开始下一任期
                    if (msPassed >= candidatExpire) {
                        currentTerm ++;
                        msPassed = 0;
                        //
                        currentTerm ++;
                        voteFor = serverID; // 第一票投给自己
                        int cnt = 1;
                        for (int i = 0; i < 5; i ++) {
                            if (i == serverID) continue;
                            try {
                                Registry registry = LocateRegistry.getRegistry("localhost", Registry.REGISTRY_PORT);
                                RaftRMI raftRMI = (RaftRMI) registry.lookup("/raft/" + i);
                                RestResult r;
                                if (logs.size() == 0) {
                                    r = raftRMI.requestVote(currentTerm, serverID, 0, 0);
                                } else {
                                    RaftLog lastLog = (RaftLog) JSON.parse(logs.get(logs.size() - 1));
                                    r = raftRMI.requestVote(currentTerm, serverID, logs.size() - 1, lastLog.getTerm());
                                }
                                if (r.isResult()) {
                                    cnt ++;
                                }
                                if (cnt >= 3) { // 选举成功，转化为leader
                                    role = RaftRole.LEADER;
                                }
                            } catch (RemoteException | NotBoundException re) {
                                log.error(re.toString());
                            }
                        }
                    }
                }
                case LEADER -> { // leader 发布心跳信号，执行同步命令
                    if (msPassed >= leaderExpire) {
                        msPassed = 0;
                        for (int i = 0; i < 5; i ++) {
                            if (i == serverID) continue;
                            try {
                                Registry registry = LocateRegistry.getRegistry("localhost", Registry.REGISTRY_PORT);
                                RaftRMI raftRMI = (RaftRMI) registry.lookup("/raft/" + i);
                                RestResult r = raftRMI.appendEntries(currentTerm, serverID, 0, 0, null, commitIndex);
                                if (!r.isResult()) {
                                    role = RaftRole.FOLLOWER;
                                }
                            } catch (RemoteException | NotBoundException re) {
                                log.error(re.toString());
                            }
                        }
                    }

                }
            }
        });
    }

    /**
     *
     * @param term term of the current leader.
     * @param leaderID leader id.
     * @param prevLogIndex the log index that is confirmed by the leader.
     * @param prevLogTerm the log term of the corresponding log index.
     * @param nxtLogs logs to be transmitted.
     * @param leaderCommit the committed index.
     * @return RestResult
     */
    public synchronized RestResult appendEntries(int term, int leaderID, int prevLogIndex, int prevLogTerm, List<String> nxtLogs, int leaderCommit) {
        msPassed = 0; // 心跳信号送达，计数器置零
        commitIndex = leaderCommit;
        this.leaderID = leaderID;
        if (term < currentTerm) {
            return new RestResult(currentTerm, false);
        }
        if (nxtLogs == null) { // 心跳信号
            if (prevLogIndex >= logs.size()) {
                return new RestResult(currentTerm, false);
            } else {
                RaftLog prevLog = (RaftLog) JSON.parse(logs.get(prevLogIndex));
                return new RestResult(currentTerm, prevLog.getTerm() == prevLogTerm);
            }
        } else {
            int nxtPtr = 0;
            while (prevLogIndex < logs.size()) {
                logs.set(prevLogIndex++, nxtLogs.get(nxtPtr++));
            }
            while (nxtPtr < nxtLogs.size()) {
                logs.add(nxtLogs.get(nxtPtr++));
            }
            return new RestResult(currentTerm, true);
        }
    }

    /**
     *
     * @param term candidate term.
     * @param candidatID candidate id.
     * @param lastLogIndex the latest log index of the current candidate.
     * @param lastLogTerm the lastest log term of the current candidate.
     * @return RestResult
     */
    public synchronized RestResult requestVote(int term, int candidatID, int lastLogIndex, int lastLogTerm) {
        if (currentTerm < term) {
            currentTerm = term;
            voteFor = 0;
        }
        if (term < currentTerm || (voteFor != 0 && voteFor != candidatID)) { // 候选人的term已过期或当前服务器已投其他候选人
            return new RestResult(currentTerm, false);
        } else if (lastLogIndex >= logs.size()) { // 候选人的日志多于当前服务器日志
            voteFor = candidatID;
            return new RestResult(currentTerm, true);
        } else { // 候选人的最新日志 term 高于当前
            RaftLog prevLog = (RaftLog) JSON.parse(logs.get(lastLogIndex));
            if (prevLog.getTerm() <= lastLogTerm) {
                voteFor = candidatID;
                return new RestResult(currentTerm, true);
            }
        }
        return new RestResult(currentTerm, false);
    }

}
