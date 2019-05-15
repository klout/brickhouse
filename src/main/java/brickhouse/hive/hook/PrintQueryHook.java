package brickhouse.hive.hook;

import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Set;

public class PrintQueryHook implements PreExecute {

    @Override
    public void run(SessionState session, Set<ReadEntity> reads,
                    Set<WriteEntity> writes, UserGroupInformation ugi) throws Exception {
        HiveHistory history = session.getHiveHistory();
    }
}
