package bean;

public class ProductBrand {
    private int id;
    private int zid;
    private int sort;
    private String title;
    private long gmtCreate;
    private long gmtModified;

    public ProductBrand() {
    }

    public ProductBrand(int id, int zid, int sort, String title, long gmtCreate, long gmtModified) {
        this.id = id;
        this.zid = zid;
        this.sort = sort;
        this.title = title;
        this.gmtCreate = gmtCreate;
        this.gmtModified = gmtModified;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getZid() {
        return zid;
    }

    public void setZid(int zid) {
        this.zid = zid;
    }

    public int getSort() {
        return sort;
    }

    public void setSort(int sort) {
        this.sort = sort;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public long getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(long gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public long getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(long gmtModified) {
        this.gmtModified = gmtModified;
    }

    @Override
    public String toString() {
        return "ProductBrand{" +
                "id=" + id +
                ", zid=" + zid +
                ", sort=" + sort +
                ", title='" + title + '\'' +
                ", gmtCreate=" + gmtCreate +
                ", gmtModified=" + gmtModified +
                '}';
    }
}
