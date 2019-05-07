#!/usr/bin/env perl

# Example:
#
# cd scsinput && ~/scs/splitfile.pl 80 < /tmp/trafficlog.txt

my $n_files = shift;
die "Number of files not supplied as first argument, aborting" unless $n_files;

my @files;
for ($i = 0; $i < $n_files; $i++) {
    my $fn = $i < 10 ? "splitfile0$i" : "splitfile$i";
    open(my $fh, '>', $fn) or die "Couldn't open file: $fn";
    $files[$i] = $fh;
}

my $j = 0;
while (<>) {
    my $fileix = ($j % $n_files);
    # print "fileix: $fileix, line: $_\n";
    print { $files[$fileix] } $_;
    $j++;
}

for my $file (@files) {
    close $file;
}
