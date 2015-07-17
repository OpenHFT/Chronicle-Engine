package net.openhft.chronicle.engine.api.management.mbean;

/**
 * Created by pct25 on 6/24/2015.
 */

import javax.management.*;
import java.io.IOException;
import java.util.*;

public class AssetTreeDynamicMBean implements DynamicMBean {

    private final Properties properties;
    private final Map attributelist;

    public AssetTreeDynamicMBean(Map attributelist) throws IOException {
        this.properties = new Properties();
        this.attributelist = attributelist;
        load();
    }



    public synchronized String getAttribute(String name) throws AttributeNotFoundException {
        String value = this.properties.getProperty(name);
        if (value != null) {
            return value;
        }
        throw new AttributeNotFoundException("No such property: " + name);
    }

    public synchronized void setAttribute(Attribute attribute) throws InvalidAttributeValueException, MBeanException, AttributeNotFoundException {
        String name = attribute.getName();
        if (this.properties.getProperty(name) == null)
            throw new AttributeNotFoundException(name);
        Object value = attribute.getValue();
        if (!(value instanceof String)) {
            throw new InvalidAttributeValueException("Attribute value not a string: " + value);
        }

        this.properties.setProperty(name, (String) value);
    }

    public void setAttribute(String name, String value) {
        Attribute attribute = new Attribute(name, value);
        AttributeList list = new AttributeList();
        list.add(attribute);
        setAttributes(list);
    }

    public synchronized AttributeList getAttributes(String[] names) {
        AttributeList list = new AttributeList();
        for (String name : names) {
            String value = this.properties.getProperty(name);
            if (value != null)
                list.add(new Attribute(name, value));
        }
        return list;
    }

    public synchronized AttributeList setAttributes(AttributeList list) {
        Attribute[] attrs = (Attribute[]) list.toArray(new Attribute[0]);
        AttributeList retlist = new AttributeList();
        for (Attribute attr : attrs) {
            String name = attr.getName();
            Object value = attr.getValue();
            if ((value instanceof String)) {
                this.properties.setProperty(name, (String) value);
                retlist.add(new Attribute(name, value));
            }
        }
        return retlist;
    }

    public Object invoke(String name, Object[] args, String[] sig) throws MBeanException, ReflectionException {
        if ((name.equals("reload")) && ((args == null) || (args.length == 0)) && ((sig == null) || (sig.length == 0))) {
            try {
                load();
                return null;
            } catch (IOException e) {
                throw new MBeanException(e);
            }
        }
        throw new ReflectionException(new NoSuchMethodException(name));
    }

    public synchronized MBeanInfo getMBeanInfo() {
        SortedSet names = new TreeSet();
        for (Iterator localIterator1 = this.properties.keySet().iterator(); localIterator1.hasNext();) {
            Object name = localIterator1.next();
            names.add((String) name);
        }

        MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[names.size()];
        Iterator it = names.iterator();

        for (int i = 0; i < attrs.length; i++) {
            String name = (String) it.next();
            attrs[i] = new MBeanAttributeInfo(name, "java.lang.String","Property " + name, true, true, false);
        }

        MBeanOperationInfo[] opers = { new MBeanOperationInfo("reload","Reload properties from file", null, "void", 1) };
        return new MBeanInfo(getClass().getName(), "Property Manager MBean",attrs, null, opers, null);
    }

    private void load() throws IOException {
        this.properties.putAll(attributelist);
    }
}