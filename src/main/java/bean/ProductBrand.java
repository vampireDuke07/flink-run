package bean;

public class ProductBrand {
    private int id;
    private int zid;
    private int sort;
    private String title;
    private String gmtCreate;
    private String gmtModified;

    public ProductBrand() {
    }

    public ProductBrand(int id, int zid, int sort, String title, String gmtCreate, String gmtModified) {
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

    public String getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(String gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public String getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(String gmtModified) {
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
