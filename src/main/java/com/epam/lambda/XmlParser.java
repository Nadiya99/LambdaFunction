package com.epam.lambda;

import com.amazonaws.util.Base64;
import com.sun.org.apache.xerces.internal.xni.parser.XMLConfigurationException;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class XmlParser {
    private Set<String> FIELDS = new HashSet<>(Arrays.asList("guid", "title", "publisher", "publish_year"));

    public Map<String, String> parseXml(String xmlFileData) throws XMLStreamException, IOException {
        Map<String, String> tags = new HashMap<>();
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        XMLEventReader eventReader = null;

        try (InputStream inputStream = new ByteArrayInputStream(Base64.decode(xmlFileData))) {
            eventReader = inputFactory.createXMLEventReader(inputStream);
            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElement = event.asStartElement();
                    String elementName = startElement.getName().getLocalPart();
                    if (FIELDS.contains(elementName)) {
                        event = eventReader.nextEvent();
                        tags.put(elementName, event.asCharacters().getData());
                    }
                }
            }
        } catch (XMLStreamException e) {
            throw new XMLStreamException(e);
        } finally {
            Objects.requireNonNull(eventReader).close();
        }

        return tags;
    }
}
