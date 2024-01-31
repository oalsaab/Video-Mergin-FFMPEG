# Video Files Mergin

Python script that attempts to merge any video files it can for a given directory.

Reading of the streams is performed with FFprobe and concatanation of files is performed with FFmpeg using [demuxer](https://trac.ffmpeg.org/wiki/Concatenate#demuxer) approach.

The order of videos merged is explictly through it's creation date, if no creation date is found then it will push the video to the end of the merged file.

If you don't care about ordering then you're probably better off with a simple bash script to create the text input to feed to FFmpeg.

You'll find that merged files are split by codecs, dimensions and audio, this is to simplify the concatantion process (the alternative is requiring wizard level understanding of concat filters).

### Usage

Make sure you have `FFprobe` & `FFmpeg` installed & accessible through env variables.

Install with poetry & run `poetry run merge <directory>`
