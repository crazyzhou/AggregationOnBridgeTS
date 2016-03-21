package cn.fudan.domain;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.HashMap;

@XmlRootElement(name = "Dtomap")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class Dtomap implements Serializable {
    private static final long serialVersionUID = -684979498775343710L;

    private HashMap<String ,Long> channelMap;

    public HashMap<String, Long> getChannelMap() {
        return channelMap;
    }

    public void setChannelMap(HashMap<String, Long> channelMap) {
        this.channelMap = channelMap;
    }
}
