package cn.fudan.tools.util;

import java.text.MessageFormat;

public class StringHelp
{
	public static String format(String str, String... s)
	{
		return new MessageFormat(str).format(s);
	}
}
