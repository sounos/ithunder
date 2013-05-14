#include <stdio.h>
#include <string.h>
#ifndef _HTMLTAG_H
#define _HTMLTAG_H
#define TAG_SIZE	    32
#define HTMLTAGS_NUM	96
#define HTMLTAG_DOCTYPE_ID	0
#define HTMLTAG_COMMENT_ID	12
#define HTMLTAG_TITLE_ID	85
#define HTMLTAG_UNKNOWN_ID	-1
#define HTMLTAG_H1_ID 3
#define HTMLTAG_P_ID 9
#define HTMLTAG_BR_ID 10
#define HTMLTAG_A_ID 43
#define HTMLTAG_IMG_ID 68
#define HTML_BLOCK_MAX   128
#define HTML_BLOCK_MIN   96
#define HTML_TEXT_MIN    96
#define HTML_PAIRS_MAX   9
#define HTML_LEVEL_MAX   2
#define HTML_FILTER_VAL  50
typedef struct _HTMLTAG
{
    int id;
    int score;
    int ispair;
    int isdelete;
    int isblock;
    char *tag;
    char *desc;
}HTMLTAG;
static HTMLTAG htmltags[] =
{
	//Basic Tags
	{0, 0, 0, 0, 0, "!doctype", "Defines the document type"},
	{1, 0, 1, 0, 0, "html", "Defines an html document"},
	{2, 0, 1, 0, 0, "body", "Defines the body element"},
	{3, 24, 1, 0, 1, "h1", "Defines header 1"},
	{4, 20, 1, 0, 1, "h2", "Defines header 2"},
	{5, 16, 1, 0, 1, "h3", "Defines header 3"},
	{6, 12, 1, 0, 1, "h4", "Defines header 4"},
	{7, 8, 1, 0, 1, "h5", "Defines header 5"},
	{8, 4, 1, 0, 1, "h6", "Defines header 6"},
	{9, 0, 1, 0, 1, "p", "Defines a paragraph"},
	{10, 0, 0, 0, 0, "br", "Inserts a single line break"},
	{11, 0, 0, 0, 0, "hr", "Defines a horizontal rule"},
	{12, 0, 0, 0, 0, "!--", "Defines a comment"},
	//Char Format
	{13, 9, 1, 0, 0, "b", "Defines bold text"},
	{14, 3, 1, 0, 0, "font", "Deprecated. Defines text font, size, and color"},
	{15, 3, 1, 0, 0, "i", "Defines italic text"},
	{16, 1, 1, 0, 0, "em", "Defines emphasized text"},
	{17, 8, 1, 0, 0, "big", "Defines big text"},
	{18, 9, 1, 0, 0, "strong", "Defines strong text"},
	{19, 1, 1, 0, 0, "small", "Defines small text"},
	{20, 0, 1, 0, 0, "sup", "Defines superscripted text"},
	{21, 0, 1, 0, 0, "sub", "Defines subscripted text"},
	{22, 0, 1, 0, 0, "bdo", "Defines the direction of text display"},
	{23, 4, 1, 0, 0, "u", "Deprecated. Defines underlined text"},
	//Output
	{24, 0, 1, 0, 1, "pre", "Defines preformatted text"},
	{25, 0, 1, 0, 1, "code", "Defines computer code text"},
	{26, 0, 1, 0, 0, "tt", "Defines teletype text"},
	{27, 0, 1, 1, 0, "kbd", "Defines keyboard text"},
	{28, 0, 1, 1, 0, "var", "Defines a variable"},
	{29, 0, 1, 1, 0, "dfn", "Defines a definition term"},
	{30, 0, 1, 1, 0, "samp", "Defines sample computer code"},
	{31, 0, 1, 1, 0, "xmp", "Deprecated. Defines preformatted text"},
	//Blocks
	{32, 0, 1, 1, 0, "acronym", "Defines an acronym"},
	{33, 0, 1, 1, 0, "abbr", "Defines an abbreviation"},
	{34, 0, 1, 1, 0, "address", "Defines an address element"},
	{35, 0, 1, 1, 0, "blockquote", "Defines a long quotation"},
	{36, 0, 1, 0, 0, "center", "Deprecated. Defines centered text"},
	{37, 0, 1, 1, 0, "q", "Defines a short quotation"},
	{38, 0, 1, 1, 0, "cite", "Defines a citation"},
	{39, 0, 1, 1, 0, "ins", "Defines inserted text"},
	{40, 0, 1, 1, 0, "del", "Defines deleted text"},
	{41, 0, 1, 1, 0, "s", "Deprecated. Defines strikethrough text"},
	{42, 0, 1, 1, 0, "strike", "Deprecated. Defines strikethrough text"},
	//Links
	{43, 12, 1, 0, 0, "a", "Defines an anchor"},
	{44, 0, 0, 0, 0, "link", "Defines a resource reference"},
	//Frames
	{45, 0, 1, 1, 0, "frame", "Defines a sub window (a frame)"},
	{46, 0, 1, 1, 0, "frameset", "Defines a set of frames"},
	{47, 0, 1, 1, 0, "noframes", "Defines a noframe section"},
	{48, 0, 1, 1, 0, "iframe", "Defines an inline sub window (frame)"},
	//Input
	{49, 0, 1, 0, 0, "form", "Defines a form"},
	{50, 0, 0, 1, 0, "input", "Defines an input field"},
	{51, 0, 1, 1, 0, "textarea", "Defines a text area"},
	{52, 0, 0, 1, 0, "button", "Defines a push button"},
	{53, 0, 1, 1, 0, "select", "Defines a selectable list"},
	{54, 0, 1, 1, 0, "optgroup", "Defines an option group"},
	{55, 0, 1, 1, 0, "option", "Defines an item in a list box"},
	{56, 0, 0, 1, 0, "label", "Defines a label for a form control"},
	{57, 0, 1, 1, 0, "fieldset", "Defines a fieldset"},
	{58, 0, 0, 1, 0, "legend", "Defines a title in a fieldset"},
	{59, 0, 0, 1, 0, "isindex", "Deprecated. Defines a single-line input field"},
	//Lists
	{60, 1, 1, 0, 1, "ul", "Defines an unordered list"},
	{61, 1, 1, 0, 1, "ol", "Defines an ordered list"},
	{62, 1, 1, 0, 1, "li", "Defines a list item"},
	{63, 1, 1, 0, 1, "dir", "Deprecated. Defines a directory list"},
	{64, 1, 1, 0, 1, "dl", "Defines a definition list"},
	{65, 0, 1, 0, 1, "dt", "Defines a definition term"},
	{66, 0, 1, 0, 1, "dd", "Defines a definition description"},
	{67, 0, 1, 1, 0, "menu", "Deprecated. Defines a menu list"},
	//Images
	{68, 1, 0, 0, 0, "img", "Defines an image"},
	{69, 1, 0, 1, 0, "map", "Defines an image map"},
	{70, 1, 0, 1, 0, "area", "Defines an area inside an image map"},
	//Tables
	{71, 0, 1, 0, 0, "table", "Defines a table"},
	{72, 0, 1, 1, 0, "caption", "Defines a table caption"},
	{73, 8, 1, 0, 1, "th", "Defines a table header"},
	{74, 0, 1, 0, 0, "tr", "Defines a table row"},
	{75, 0, 1, 0, 1, "td", "Defines a table cell"},
	{76, 8, 1, 0, 1, "thead", "Defines a table header"},
	{77, 0, 1, 0, 1, "tbody", "Defines a table body"},
	{78, 0, 1, 0, 1, "tfoot", "Defines a table footer"},
	{79, 0, 1, 1, 0, "col", "Defines attributes for table columns"},
	{80, 0, 1, 1, 0, "colgroup", "Defines groups of table columns"},
	//Styles
	{81, 0, 1, 1, 0, "style", "Defines a style definition"},
	{82, 0, 1, 0, 1, "div", "Defines a section in a document"},
	{83, 0, 1, 0, 0, "span", "Defines a section in a document"},
	//Meta Info
	{84, 0, 1, 0, 0, "head", "Defines information about the document"},
	{85, 64, 1, 0, 0, "title", "Defines the document title"},
	{86, 0, 0, 1, 0, "meta", "Defines meta information"},
	{87, 0, 0, 1, 0, "base", "Defines a base URL for all the links in a page"},
	{88, 0, 0, 1, 0, "basefont", "Deprecated. Defines a base font"},
	//Programming
	{89, 0, 1, 1, 0, "script", "Defines a script"},
	{90, 0, 1, 1, 0, "noscript", "Defines a noscript section"},
	{91, 0, 1, 1, 0, "applet", "Deprecated. Defines an applet"},
	{92, 0, 1, 1, 0, "object", "Defines an embedded object"},
	{93, 0, 1, 1, 0, "param", "Defines a parameter for an object"},
	{94, 0, 1, 1, 0, "marquee", "Defines action marquee"},
	{95, 0, 0, 1, 0, "embed", "Defines action embed"},
};
#endif
