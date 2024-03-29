package fogetti.phish.storm.relatedness.suffix;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import fogetti.phish.storm.exception.URLMatchingFailedException;

public class PublicSuffixMatcherTest {

	@Test
	public void canCreate() throws Exception {
		// Given
		PublicSuffixMatcher reader = new PublicSuffixMatcher("/some/path");

		// When

		// Then
		assertNotNull("The created reader was null", reader);
	}
	
	@Test(expected = URLMatchingFailedException.class)
	public void noRead() throws Exception {
		// Given we want to match URLs to public suffixes
		PublicSuffixMatcher reader = new PublicSuffixMatcher("/some/path");

		// When we don't read the data from the file

		// Then the match fails
		reader.findPublicSuffix("some.domain");
	}
	
	@Test(expected = URLMatchingFailedException.class)
	public void invalidLocation() throws Exception {
		// Given we want to match URLs to public suffixes

		// When we provide an invalid data file location
		PublicSuffixMatcher reader = new PublicSuffixMatcher("/some/path");

		// Then the load fails
		reader.load();
	}

	@Test
	public void noData() throws Exception {
		String empty = new File(this.getClass().getClassLoader().getResource("empty.dat").toURI()).getAbsolutePath();
		// Given we want to match URLs to public suffixes
		PublicSuffixMatcher reader = new PublicSuffixMatcher(empty);

		// When we read the data from the file but the file is empty
		reader.load();

		// Then the match returns an "*" string
		String ps = reader.findPublicSuffix("some.domain");
		assertEquals("The returned public suffix was not 'domain'", "domain", ps);
	}
	
	@Test
	public void onlyComments() throws Exception {
		String onlycomments = new File(this.getClass().getClassLoader().getResource("only-comments.dat").toURI()).getAbsolutePath();
		// Given we want to match URLs to public suffixes
		PublicSuffixMatcher reader = new PublicSuffixMatcher(onlycomments);

		// When we read the data from the file but the file contains only whitespace and comment
		reader.load();

		// Then the match returns an "*" string
		String ps = reader.findPublicSuffix("some.domain");
		assertEquals("The returned public suffix was not 'domain'", "domain", ps);
	}
	
	@Test
	public void match() throws Exception {
		String psdat = new File(this.getClass().getClassLoader().getResource("public-suffix-list.dat").toURI()).getAbsolutePath();
		// Given we want to match URLs to public suffixes
		PublicSuffixMatcher reader = new PublicSuffixMatcher(psdat);

		// When we read the data from the file but the file contains only whitespace and comment
		reader.load();

		// Then the match returns an empty string
		boolean match = reader.match("fogetti.phis.storm.paragliding.aero");
		assertTrue("The provided MLD did not match any public suffix", match);
	}

	@Test
	public void found() throws Exception {
		String psdat = new File(this.getClass().getClassLoader().getResource("public-suffix-list.dat").toURI()).getAbsolutePath();
		// Given we want to match URLs to public suffixes
		PublicSuffixMatcher reader = new PublicSuffixMatcher(psdat);

		// When we read the data from the file but the file contains only whitespace and comment
		reader.load();

		// Then the match returns an empty string
		String ps = reader.findPublicSuffix("fogetti.phis.storm.paragliding.aero");
		assertEquals("The returned public suffix was not 'paragliding.aero'", "paragliding.aero", ps);
	}

	@Test
    public void funkyURL() throws Exception {
        // Given
        String psdat = new File(this.getClass().getClassLoader().getResource("public-suffix-list.dat").toURI()).getAbsolutePath();
        PublicSuffixMatcher reader = new PublicSuffixMatcher(psdat);
	    
        // When
        reader.load();
        String URL = "www.municode.com/library/mi/benton_charter_township,_(berrien_co.)/code_of_ordinances";
        String mldps = URL.split("/")[0];
        String ps = reader.findPublicSuffix(mldps);
        String beforeLast = StringUtils.substringBeforeLast(mldps, "." + ps);
        String mld = beforeLast;
        if (beforeLast.contains(".")) mld = StringUtils.substringAfterLast(beforeLast, ".");
        
        // Then
        assertEquals("The returned mld was wrong", "municode", mld);
        assertEquals("The returned mldps was wrong", "municode.com", mld+"."+ps);
    }
}
