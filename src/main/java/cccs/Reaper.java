package cccs;

import org.apache.iceberg.Table;

public class Reaper {

    private Table table;

    public Reaper() throws Exception {
        loadTable();
    }

    public void loadTable() throws Exception {
        this.table = Constants.tables.load(Constants.tableLocation);
    }

    public long expireSnapshots() {
        long start = System.currentTimeMillis();
        long tsToExpire = System.currentTimeMillis() - (10 * 60 * 1000); // 10 minute
        this.table.expireSnapshots()
                .expireOlderThan(tsToExpire)
                .retainLast(20)
                .commit();
        long end = System.currentTimeMillis();
        System.out.println("Time to expire old snapshots (ms): " + (end - start));
        return (end - start);
    }
}
