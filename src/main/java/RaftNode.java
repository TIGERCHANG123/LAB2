
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
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
    private final ExecutorService es = Executors.newFixedThreadPool(8);

    private volatile RaftRole role = RaftRole.FOLLOWER;
    private volatile int msPassed = 0; // 计时
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
    private int lastApplied; // 上位机使用
    private volatile boolean onVoting; // 投票可能有多轮，下一轮投票开始之后上一轮投票线程应释放。
    // Raft leader state
    private final int[] nextIndex = new int[5];
    private final int[] matchIndex = new int[5];

    public RaftNode(int id) throws RemoteException {
        serverID = id;
        roleSwitch(RaftRole.CANDIDATE);
        roleSwitch(RaftRole.FOLLOWER);
        while (true) {
            try {
                Thread.sleep(1000); // 休眠 1ms 调试时设置为1s
                msPassed ++;
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
            switch (role) {
                case FOLLOWER -> { // follower 超时，开启选举操作
                    if (msPassed >= followerExpire) {
                        msPassed = 0;
                        roleSwitch(RaftRole.CANDIDATE);
                        voteFor = serverID; // 第一票投给自己
                        while (role == RaftRole.CANDIDATE) { // 每次轮选举持续 candidateExpire 时间
                            CountDownLatch cdl = new CountDownLatch(2);
                            onVoting = true;
                            currentTerm ++;
                            for (int i = 0; i < 5; i ++) {
                                if (i == serverID) continue;
                                requestVoteLauncher(i, cdl);
                            }
                            try {
                                cdl.wait(candidatExpire);
                                onVoting = false; // 取消所有的投票线程
                                if (cdl.getCount() >= 2) { // 投票成功 转化为leader并进行leader的初始化工作，开始同步数据
                                    roleSwitch(RaftRole.LEADER);
                                    Arrays.fill(matchIndex, -1);
                                    Arrays.fill(nextIndex, logs.size());
                                    for (int i = 0; i < 5; i ++) { // 开启数据同步线程。注意这里和原论文有出入，分离心跳信号和同步操作
                                        if (i == serverID) continue;
                                        appendEntriesLauncher(i);
                                    }
                                }
                            } catch (InterruptedException e) {
                                log.error(e.toString());
                            }
                        }
                    }
                }
                case LEADER -> { // leader 发布心跳信号
                    if (msPassed >= leaderExpire) {
                        msPassed = 0;
                        for (int i = 0; i < 5; i ++) { // 开启数据同步线程。注意这里和原论文有出入，分离心跳信号和同步操作
                            if (i == serverID) continue;
                            RestResult heartbeat = appendEntries(currentTerm, serverID, 0, 0, null, commitIndex);
                            if (heartbeat.getTerm() > currentTerm) { // 存在新一轮选举，说明当前leader已经过期
                                roleSwitch(RaftRole.FOLLOWER);
                                break;
                            }
                        }
                    }
                }
            }
        }

    }

    /**
     * if (term == currentTerm && logs[prevLogIndex] exits and matches prevLogTerm), return true, else return false.
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
        roleSwitch(RaftRole.FOLLOWER);
        if (term < currentTerm) {
            return new RestResult(currentTerm, false);
        }
        if (nxtLogs == null) { // 心跳信号
            return new RestResult(currentTerm, true);
        } else if (prevLogIndex >= logs.size()) { //
            return new RestResult(currentTerm, false);
        } else if (prevLogIndex == -1) { // 当前logs为空
            logs.clear();
            logs.addAll(nxtLogs);
            return new RestResult(currentTerm, true);
        } else {
            RaftLog raftLog = (RaftLog) JSON.parse(logs.get(prevLogIndex));
            if (raftLog.getTerm() == prevLogTerm) { // 当前最新的 log 与 leader 匹配，则将剩余的log加入末尾
                int ts = logs.size();
                for (int di = prevLogIndex + 1; di < ts; di ++) { // 删除不匹配的日志项
                    logs.remove(logs.size() - 1);
                }
                logs.addAll(nxtLogs);
                return new RestResult(currentTerm, true);
            } else {
                return new RestResult(currentTerm, false);
            }
        }
    }

    private void appendEntriesLauncher(int i) {
        LinkedList<String> nxtLogs = new LinkedList<>(); // 此处可考虑使用下标，是一个空间和时间的 trade-off
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", Registry.REGISTRY_PORT);
            RaftRMI raftRMI = (RaftRMI) registry.lookup("/raft/" + i);
            while (role == RaftRole.LEADER) {
                RaftLog raftLog = (RaftLog) JSON.parse(logs.get(nextIndex[i]));
                RestResult r = raftRMI.appendEntries(currentTerm, serverID, nextIndex[i], raftLog.getTerm(), nxtLogs, commitIndex);
                if (r.getTerm() > currentTerm) { // 出现新的任期，修改当前服务器角色为FOLLOWER并退出同步线程
                    roleSwitch(RaftRole.FOLLOWER);
                    break;
                }
                if (r.isResult()) { // prevLogIndex确认，更新 commitIndex，清空传输队列
                    matchIndex[i] = nextIndex[i];
                    nextIndex[i] ++;
                    nxtLogs.clear();
                    int[] tmp = Arrays.copyOf(matchIndex, 5);
                    Arrays.sort(tmp);
                    commitIndex = Math.max(commitIndex, tmp[3]); // commitIndex 单增
                } else { // 否则递减 nextIndex 并将需要的额nxtLogs添加到数组中
                    nextIndex[i] --;
                    nxtLogs.addFirst(logs.get(nextIndex[i]));
                }
            }
        } catch (RemoteException | NotBoundException re) {
            log.error(re.toString());
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
    public RestResult requestVote(int term, int candidatID, int lastLogIndex, int lastLogTerm) {
        if (currentTerm < term) {
            currentTerm = term;
            roleSwitch(RaftRole.FOLLOWER);
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

    private void requestVoteLauncher(int i, CountDownLatch cdl) {
        es.submit(()-> {
            while (onVoting && role == RaftRole.CANDIDATE) {
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
                        cdl.countDown();
                    }
                } catch (RemoteException | NotBoundException re) {
                    log.error(re.toString());
                }
            }
        });
    }

    /**
     * perform exchange of role. assign new random expire time.
     * @param newRole
     */
    private void roleSwitch(RaftRole newRole) {
        Random rand = new Random();
        switch (newRole) {
            case LEADER -> {
                role = RaftRole.LEADER;
            }
            case FOLLOWER -> {
                role = RaftRole.FOLLOWER;
                followerExpire = rand.nextInt(50) + 50;
            }
            case CANDIDATE -> {
                role = RaftRole.CANDIDATE;
                candidatExpire = rand.nextInt(50) + 50;
            }
        }
    }
}
