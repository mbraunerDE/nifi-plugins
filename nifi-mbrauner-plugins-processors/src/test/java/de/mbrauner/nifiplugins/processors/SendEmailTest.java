package de.mbrauner.nifiplugins.processors;

import com.icegreen.greenmail.store.FolderException;
import com.icegreen.greenmail.user.UserException;
import groovy.util.logging.Slf4j;
import jakarta.mail.Header;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.*;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

import com.icegreen.greenmail.junit4.*;
import com.icegreen.greenmail.util.*;
import com.icegreen.greenmail.*;

public class SendEmailTest {

    @Rule public final GreenMailRule greenMail = new GreenMailRule(ServerSetupTest.ALL);
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SendEmailTest.class);
    private TestRunner testRunner;

    @After public void after() throws FolderException {
        greenMail.purgeEmailFromAllMailboxes();
        greenMail.stop();
    }

    @Before public void before() throws UserException {
        greenMail.getUserManager().createUser("send@test.test", "send@test.test", "send");
        greenMail.getUserManager().createUser("receive@test.test", "receive@test.test", "receive");
        greenMail.start();

        testRunner = TestRunners.newTestRunner(SendEmail.class);
        testRunner.clearProperties();
        testRunner.setProperty(SendEmail.SMTP_HOSTNAME, greenMail.getSmtp().getBindTo());
        testRunner.setProperty(SendEmail.SMTP_PORT, Integer.toString(greenMail.getSmtp().getPort()));
        testRunner.setProperty(SendEmail.SMTP_USERNAME, "send@test.test");
        testRunner.setProperty(SendEmail.SMTP_PASSWORD, "send");
        testRunner.setProperty(SendEmail.FROM, "send@test.test");
        testRunner.setProperty(SendEmail.TO, "receive@test.test");
        testRunner.setValidateExpressionUsage(false);

        MockFlowFile ff = new MockFlowFile(1);
        ff.setData("content content content".getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(ff);
    }

    @Test public void checkNotSet() throws MessagingException {
        run();
        MimeMessage mail = getMail();
        assertThat(mail.getHeader("X-Mailer")).containsOnly("NiFi");
        assertThat(mail.getHeader("gibt es nicht")).isNull();
    }

    private PropertyDescriptor propertyDescriptor(String name){
        return new PropertyDescriptor.Builder().name(name).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).dynamic(true).build();
    }

    @Test public void checkEmpty() throws MessagingException {
        testRunner.setProperty(propertyDescriptor("emptyProperty") ,"");
        run();
        MimeMessage mail = getMail();
        assertThat(mail.getHeader("X-Mailer")).containsOnly("NiFi");
        assertThat(mail.getHeader("gibt es nicht")).isNull();
        assertThat(mail.getHeader("X-emptyProperty")).containsExactly("");
    }

    @Test public void checkSingle() throws MessagingException {
        testRunner.setProperty(propertyDescriptor("schlüssel") ,"wert");
        run();
        MimeMessage mail = getMail();
        Enumeration<Header> e = mail.getAllHeaders();
        while (e.hasMoreElements()) {
            Header h = e.nextElement();
            log.info("found '{}' with '{}'", h.getName(), h.getValue());
        }
        assertThat(mail.getHeader("X-Mailer")).containsOnly("NiFi");
        assertThat(mail.getHeader("X-schlssel")).containsOnly("wert");
        assertThat(mail.getHeader("gibt es nicht")).isNull();
    }

    @Test public void checkMulti() throws MessagingException {
        testRunner.setProperty(propertyDescriptor("schlüssel") ,"wert");
        testRunner.setProperty(propertyDescriptor("key") ,"value");
        run();
        MimeMessage mail = getMail();
        assertThat(mail.getHeader("X-Mailer")).containsOnly("NiFi");
        assertThat(mail.getHeader("X-schlssel")).containsOnly("wert");
        assertThat(mail.getHeader("X-key")).containsOnly("value");
        assertThat(mail.getHeader("gibt es nicht")).isNull();
    }

    private MimeMessage getMail() {
        MimeMessage[] mails = greenMail.getReceivedMessages();
        assertThat(mails).hasSize(1);
        return mails[0];
    }

    private void run() {
        testRunner.assertValid();
        testRunner.run();
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(SendEmail.REL_SUCCESS);
    }
}
