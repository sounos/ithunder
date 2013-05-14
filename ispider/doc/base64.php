#!/usr/bin/php
<?php
$self = $_SERVER['argv'][0];
if($_SERVER['argc'] < 3)
{
    echo "Usage:$self infile outfile\n";
    exit;
}
$infile = $_SERVER['argv'][1];
$outfile = $_SERVER['argv'][2];
if(($text = file_get_contents($infile)))
{
    $base64code = base64_encode($text);
    $outtext = "#ifndef _BASE64_HTML_H\n";
    $outtext .= "#define _BASE64_HTML_H\n";
    $outtext .= "static const char *html_code_base64 = \"$base64code\";\n";
    $outtext .= "#endif\n";
    if(file_exists($outfile)) @unlink($outfile);
    file_put_contents($outfile, $outtext);
}
?>
